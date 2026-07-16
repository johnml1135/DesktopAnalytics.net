using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DiskQueue;

namespace DesktopAnalytics
{
	/// <summary>
	/// The outcome of attempting to deliver one batch of spooled events, as reported by the caller
	/// of <see cref="EventSpool.ProcessBatchAsync"/>. This drives whether the batch is removed from
	/// the spool (<see cref="Delivered"/>/<see cref="PoisonDrop"/>) or left in place for a later
	/// retry (<see cref="RetryableFailure"/>). (Per-record rejections within a processed batch are
	/// the sender/client's concern -- see <see cref="BatchSendResult.FailedIndices"/> -- and count
	/// as <see cref="Delivered"/> here: the batch is finished with either way.)
	/// </summary>
	internal enum SendResult
	{
		/// <summary>The batch was processed by the server (e.g. HTTP 2xx, or a strict-mode 400
		/// where the rejected records are individually reported). Remove it from the spool.</summary>
		Delivered,

		/// <summary>A transient failure (connection error, timeout, 5xx, 429). Leave the batch in
		/// the spool for a later retry.</summary>
		RetryableFailure,

		/// <summary>A non-retryable failure (e.g. a 4xx that will never succeed). Remove the batch
		/// from the spool anyway so bad events cannot wedge the spool forever.</summary>
		PoisonDrop
	}

	/// <summary>
	/// Durable, bounded, on-disk store of pending <see cref="AnalyticsEvent"/>s, backed by
	/// DiskQueue. Events are enqueued as opaque UTF-8 JSON bytes (see
	/// <see cref="AnalyticsEvent.ToBytes"/>) so the spool stays portable and inspectable.
	/// </summary>
	/// <remarks>
	/// Every public method here swallows disk/IO/serialization exceptions internally (logging via
	/// <see cref="Debug.WriteLine(string)"/>) and returns gracefully -- analytics code must never
	/// throw into the host application. The one exception is the constructor: failing to acquire
	/// the underlying DiskQueue's cross-process exclusive lock is a startup-time condition the
	/// caller needs to know about, so it is allowed to propagate.
	/// </remarks>
	internal class EventSpool : IDisposable
	{
		// How long we wait to acquire DiskQueue's cross-process exclusive lock when opening the
		// spool. Kept short: if another process is holding the lock for this long, something is
		// wrong, and we'd rather fail fast than hang application startup/shutdown.
		private static readonly TimeSpan s_lockWaitTimeout = TimeSpan.FromSeconds(2);

		// Number of leading bytes of the SHA-256 hash of the API key used to build the spool
		// directory name. Just needs to be stable and distinguish keys from each other -- it is
		// not a security boundary -- so a short prefix is plenty.
		private const int kApiKeyHashBytes = 8;

		private readonly int _maxItems;
		private readonly int _maxItemBytes;
		private readonly long _maxSpoolBytes;
		// Logical bytes of the LIVE entries (disk file sizes would overcount consumed-but-untrimmed
		// ones). Guarded by _sync; measured at open, then maintained incrementally.
		private long _spoolBytes;
		private readonly string _spoolDirectory;
		// Not readonly: Purge replaces the queue wholesale (see its remarks). Always accessed
		// under _sync. Null only if a Purge-time reopen failed; every public method already
		// catches and logs whatever follows from that.
		private IPersistentQueue _queue;
		// Serializes ALL access to _queue, exactly like the lock(_syncRoot) it replaced -- a
		// SemaphoreSlim because ProcessBatchAsync must hold it across awaits of the send callback,
		// which a monitor lock cannot. Deliberately never disposed: it holds no unmanaged
		// resources unless AvailableWaitHandle is touched (it isn't), and disposing it would turn
		// a benign use-after-Dispose race into ObjectDisposedException noise.
		private readonly SemaphoreSlim _sync = new SemaphoreSlim(1, 1);
		private bool _disposed;

		/// <summary>
		/// Raised once for each event permanently dropped by cap enforcement
		/// (<see cref="EnforceCapLocked"/>) -- i.e. NOT a drop at enqueue time, which
		/// <see cref="Enqueue"/>'s own return value already reports, but an OLDER event (or, in the
		/// degenerate <c>maxItems == 0</c> case, the just-enqueued one) evicted later to keep the
		/// spool within its configured caps. This is the only way callers can learn about that kind
		/// of drop, since it can happen on an <see cref="Enqueue"/> call for a DIFFERENT event than
		/// the one being dropped. Raised while the internal lock is held, so handlers must be fast
		/// and must not call back into this <see cref="EventSpool"/>.
		/// </summary>
		public event Action ItemDroppedByCap;

		/// <summary>
		/// Opens (creating if necessary) the on-disk event spool rooted at
		/// <paramref name="spoolDirectory"/>, taking DiskQueue's cross-process exclusive lock for
		/// the lifetime of this object.
		/// </summary>
		/// <param name="spoolDirectory">Directory to hold the spool's files. Callers normally get
		/// this from <see cref="GetDefaultSpoolPath"/>; tests typically inject a temp directory.</param>
		/// <param name="maxItems">The maximum number of events retained. Enqueuing beyond this
		/// drops the oldest event(s) first.</param>
		/// <param name="maxItemBytes">The maximum serialized size of a single event; anything
		/// larger is refused at enqueue time (<see cref="Enqueue"/> returns false) rather than
		/// spooled. Defaults to unbounded; <see cref="MixpanelClient"/> passes the transport's
		/// real per-event limit.</param>
		/// <param name="maxSpoolBytes">The maximum total serialized size of all retained events;
		/// enqueuing beyond this drops the oldest event(s) first, exactly like
		/// <paramref name="maxItems"/>. Bounds both the disk footprint and the total upload
		/// liability of an accumulated backlog. Defaults to unbounded.</param>
		/// <exception cref="Exception">Propagates whatever DiskQueue throws if the lock cannot be
		/// acquired within the internal wait timeout (e.g. another process/instance already has
		/// this spool open) or if the directory cannot be created/opened. This is deliberately not
		/// swallowed: it is a startup-time failure the caller needs to know about.</exception>
		public EventSpool(string spoolDirectory, int maxItems, int maxItemBytes = int.MaxValue,
			long maxSpoolBytes = long.MaxValue)
		{
			if (maxItems < 0)
				throw new ArgumentOutOfRangeException(nameof(maxItems));
			if (maxItemBytes <= 0)
				throw new ArgumentOutOfRangeException(nameof(maxItemBytes));
			if (maxSpoolBytes <= 0)
				throw new ArgumentOutOfRangeException(nameof(maxSpoolBytes));

			_maxItems = maxItems;
			_maxItemBytes = maxItemBytes;
			_maxSpoolBytes = maxSpoolBytes;
			_spoolDirectory = spoolDirectory;
			_queue = PersistentQueue.WaitFor(spoolDirectory, s_lockWaitTimeout);
			_spoolBytes = ResolveExistingSpoolBytes();
		}

		private string SpoolBytesFilePath => Path.Combine(_spoolDirectory, "spool-bytes.txt");

		// Cheap common-case restart path: reuses the byte total PersistSpoolBytesLocked wrote during
		// the previous session instead of re-measuring by walking (dequeuing and rolling back) every
		// live entry, which is the only way to get an exact total (see MeasureExistingSpoolBytes) but
		// means a large backlog turns every app startup into a synchronous, disk-bound scan. The
		// persisted total is cross-checked against DiskQueue's own (already-cheap)
		// EstimatedCountOfItemsInQueue -- "queue is empty" disagreeing with "persisted total is
		// nonzero" (or vice versa) means the sidecar file is stale (e.g. the previous session crashed
		// between enqueuing and persisting) and is not trusted, falling back to the slow but exact
		// measurement.
		private long ResolveExistingSpoolBytes()
		{
			try
			{
				var text = File.ReadAllText(SpoolBytesFilePath);
				if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var persisted) &&
					persisted >= 0)
				{
					var isEmpty = _queue.EstimatedCountOfItemsInQueue <= 0;
					if (isEmpty == (persisted == 0))
						return persisted;
				}
			}
			catch (Exception e)
			{
				Debug.WriteLine("EventSpool: no usable persisted spool byte total, measuring instead: " + e);
			}

			var measured = MeasureExistingSpoolBytes();
			PersistSpoolBytesLocked(measured);
			return measured;
		}

		// Sums the live entries left by a previous session (dequeue-all in a session disposed
		// without Flush = pure measurement; DiskQueue rolls it back). Failure => 0: the byte cap
		// is then under-enforced until the backlog drains, erring toward keeping events.
		private long MeasureExistingSpoolBytes()
		{
			try
			{
				long total = 0;
				using (var session = _queue.OpenSession())
				{
					while (true)
					{
						var bytes = session.Dequeue();
						if (bytes == null)
							break;
						total += bytes.Length;
					}
				}
				return total;
			}
			catch (Exception e)
			{
				Debug.WriteLine("EventSpool: failed to measure existing spool bytes, assuming 0: " + e);
				return 0;
			}
		}

		// Caller must hold _sync (or be under construction, before _sync can be contended).
		// Best-effort: a failure here just means the NEXT open falls back to the slower
		// MeasureExistingSpoolBytes instead of this fast path -- never a correctness problem.
		private void PersistSpoolBytesLocked(long spoolBytes)
		{
			try
			{
				File.WriteAllText(SpoolBytesFilePath, spoolBytes.ToString(CultureInfo.InvariantCulture));
			}
			catch (Exception e)
			{
				Debug.WriteLine("EventSpool: failed to persist spool byte total: " + e);
			}
		}

		/// <summary>
		/// Computes the per-user, per-API-key spool directory:
		/// %LocalAppData%\SIL\DesktopAnalytics\spool\&lt;short hash of apiKey&gt;. The API key is
		/// hashed (never embedded raw) so that, e.g., DEBUG and RELEASE builds of an app -- which
		/// use different keys -- get separate spools, without exposing the key via the file system.
		/// </summary>
		public static string GetDefaultSpoolPath(string apiKey)
		{
			var root = Path.Combine(
				Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
				"SIL", "DesktopAnalytics", "spool");
			return Path.Combine(root, HashApiKey(apiKey));
		}

		private static string HashApiKey(string apiKey)
		{
			using (var sha = SHA256.Create())
			{
				var hash = sha.ComputeHash(Encoding.UTF8.GetBytes(apiKey ?? string.Empty));
				var builder = new StringBuilder(kApiKeyHashBytes * 2);
				for (var i = 0; i < kApiKeyHashBytes; i++)
					builder.Append(hash[i].ToString("x2"));
				return builder.ToString();
			}
		}

		/// <summary>
		/// An approximate count of events currently in the spool. "Approximate" because DiskQueue
		/// tracks this as an estimate that is cheap to read; it is accurate enough for bounding and
		/// diagnostics but should not be treated as a hard guarantee under concurrent access.
		/// </summary>
		public int ApproximateCount
		{
			get
			{
				try
				{
					_sync.Wait();
					try
					{
						return _queue.EstimatedCountOfItemsInQueue;
					}
					finally
					{
						_sync.Release();
					}
				}
				catch (Exception e)
				{
					Debug.WriteLine("EventSpool.ApproximateCount: failed to read count: " + e);
					return 0;
				}
			}
		}

		/// <summary>
		/// Serializes and enqueues <paramref name="evt"/>, then enforces the configured item cap by
		/// dropping the oldest event(s) if necessary. Never throws: any disk/IO/serialization
		/// failure is logged and swallowed, and the event is simply lost rather than crashing the
		/// host.
		/// </summary>
		/// <returns>True if the event was durably enqueued; false if it was dropped at enqueue time
		/// (null, unserializable, larger than the per-event byte cap, or a disk failure). Callers use
		/// this to count that kind of drop -- see <see cref="MixpanelClient.Track"/>. A later drop by
		/// cap enforcement (this call's own item, or some earlier one) is reported separately via
		/// <see cref="ItemDroppedByCap"/>, since it does not necessarily happen on the enqueue call
		/// for the event that gets dropped.</returns>
		public bool Enqueue(AnalyticsEvent evt)
		{
			if (evt == null)
				return false;

			try
			{
				byte[] bytes;
				try
				{
					bytes = evt.ToBytes();
				}
				catch (Exception e)
				{
					Debug.WriteLine("EventSpool.Enqueue: failed to serialize event, dropping: " + e);
					return false;
				}

				if (bytes.Length > _maxItemBytes)
				{
					// Refused up front rather than spooled: an event over the transport's per-event
					// limit can never be delivered, and if it is large enough to time out the HTTP
					// request it would be classified as a RETRYABLE failure (a timeout is
					// indistinguishable from being offline) and wedge the head of the queue forever.
					Debug.WriteLine("EventSpool.Enqueue: event of " + bytes.Length +
						" bytes exceeds the " + _maxItemBytes + "-byte cap, dropping: " + evt.EventName);
					return false;
				}

				_sync.Wait();
				try
				{
					using (var session = _queue.OpenSession())
					{
						session.Enqueue(bytes);
						session.Flush();
					}

					_spoolBytes += bytes.Length;
					EnforceCapLocked();
					PersistSpoolBytesLocked(_spoolBytes);
				}
				finally
				{
					_sync.Release();
				}

				return true;
			}
			catch (Exception e)
			{
				Debug.WriteLine("EventSpool.Enqueue failed: " + e);
				return false;
			}
		}

		// Caller must hold _sync. Drops the oldest item(s) one at a time until the queue is at or
		// under BOTH caps (_maxItems and _maxSpoolBytes). Each iteration removes exactly one item
		// (or gives up on a read/dequeue failure), so this always terminates -- including the
		// degenerate case of _maxItems == 0, where even the item just enqueued is dropped.
		private void EnforceCapLocked()
		{
			while (true)
			{
				int count;
				try
				{
					count = _queue.EstimatedCountOfItemsInQueue;
				}
				catch (Exception e)
				{
					Debug.WriteLine("EventSpool.EnforceCap: failed to read count, giving up: " + e);
					return;
				}

				if (count <= _maxItems && _spoolBytes <= _maxSpoolBytes)
					return;

				if (count <= 0)
				{
					// Nothing left to drop; if the byte estimate says otherwise it has drifted --
					// resync it to the truth (an empty queue holds zero bytes).
					_spoolBytes = 0;
					return;
				}

				try
				{
					using (var session = _queue.OpenSession())
					{
						var oldest = session.Dequeue();
						if (oldest == null)
						{
							_spoolBytes = 0; // See the count <= 0 case above.
							return;
						}
						session.Flush();
						_spoolBytes = Math.Max(0, _spoolBytes - oldest.Length);
					}

					try
					{
						ItemDroppedByCap?.Invoke();
					}
					catch (Exception e)
					{
						Debug.WriteLine("EventSpool.EnforceCap: ItemDroppedByCap handler threw: " + e);
					}
				}
				catch (Exception e)
				{
					Debug.WriteLine("EventSpool.EnforceCap: failed to drop oldest item, giving up: " + e);
					return;
				}
			}
		}

		// Test seam: the spool's current logical byte total (the value the byte cap is enforced
		// against), primarily so tests can prove it survives a dispose/reopen.
		internal long ApproximateBytes
		{
			get
			{
				_sync.Wait();
				try
				{
					return _spoolBytes;
				}
				finally
				{
					_sync.Release();
				}
			}
		}

		/// <summary>
		/// Gathers up to <paramref name="maxItems"/> spooled events (oldest first, bounded by
		/// <paramref name="maxBytes"/>) in a single DiskQueue session/transaction, hands them to
		/// <paramref name="send"/> as ONE batch (one network request -- see
		/// <see cref="MixpanelEventSender"/>), and commits or rolls back the whole batch on its
		/// verdict:
		/// <list type="bullet">
		/// <item><see cref="SendResult.Delivered"/> or <see cref="SendResult.PoisonDrop"/>: every
		/// dequeue is committed (<c>session.Flush()</c>) -- the batch is finished with, whether
		/// ingested or permanently rejected.</item>
		/// <item><see cref="SendResult.RetryableFailure"/>: nothing is committed -- disposing the
		/// session without flushing rolls the whole batch back into the spool, in order, for a
		/// later retry.</item>
		/// </list>
		/// If <paramref name="send"/> throws, it is treated exactly like
		/// <see cref="SendResult.RetryableFailure"/>: the exception is swallowed and nothing is
		/// flushed. This is the crash-window path that guarantees at-least-once delivery -- a
		/// sender that delivered the batch but crashed/threw before this method could flush will
		/// see the same events again on the next call.
		/// An entry that cannot be deserialized (corrupt, or an incompatible schema version) is
		/// skipped during the gather and its removal is committed together with the batch (or, if
		/// nothing else was gathered, on its own) -- it can never wedge the spool.
		/// </summary>
		/// <param name="maxItems">Maximum number of events gathered into this call's batch.</param>
		/// <param name="send">Decides the batch's fate; see the summary above. Receives
		/// <paramref name="cancellationToken"/> so the send can participate in cancellation.</param>
		/// <param name="maxBytes">Soft byte budget for this call's batch, bounding each drain burst
		/// on a slow/metered connection. Checked AFTER each gathered event, so a single event over
		/// the whole budget still goes -- the budget can never starve the queue.</param>
		/// <param name="cancellationToken">Bounds the wait for the spool's internal semaphore and
		/// is passed through to <paramref name="send"/>. Cancellation never throws out of this
		/// method; it just means the batch (if any was gathered) rolls back.</param>
		public async Task ProcessBatchAsync(int maxItems,
			Func<IReadOnlyList<AnalyticsEvent>, CancellationToken, Task<SendResult>> send,
			long maxBytes = long.MaxValue, CancellationToken cancellationToken = default)
		{
			if (send == null || maxItems <= 0 || maxBytes <= 0)
				return;

			try
			{
				await _sync.WaitAsync(cancellationToken).ConfigureAwait(false);
				try
				{
					using (var session = _queue.OpenSession())
					{
						var batch = new List<AnalyticsEvent>();
						long batchBytes = 0;
						// Bytes of undeserializable entries dequeued (skipped) during the gather;
						// committed along with the batch.
						long skippedBytes = 0;

						while (batch.Count < maxItems && batchBytes < maxBytes)
						{
							byte[] bytes;
							try
							{
								bytes = session.Dequeue();
							}
							catch (Exception e)
							{
								Debug.WriteLine("EventSpool.ProcessBatch: dequeue failed: " + e);
								break; // Send whatever was gathered before the failure.
							}

							if (bytes == null)
								break; // Spool is empty.

							try
							{
								batch.Add(AnalyticsEvent.FromBytes(bytes));
								batchBytes += bytes.Length;
							}
							catch (Exception e)
							{
								Debug.WriteLine(
									"EventSpool.ProcessBatch: failed to deserialize event, dropping: " + e);
								skippedBytes += bytes.Length;
							}
						}

						if (batch.Count == 0)
						{
							if (skippedBytes > 0)
							{
								// Nothing to send, but bad entries were dequeued -- commit their removal.
								session.Flush();
								_spoolBytes = Math.Max(0, _spoolBytes - skippedBytes);
								PersistSpoolBytesLocked(_spoolBytes);
							}
							return;
						}

						SendResult result;
						try
						{
							result = await send(batch, cancellationToken).ConfigureAwait(false);
						}
						catch (Exception e)
						{
							Debug.WriteLine(
								"EventSpool.ProcessBatch: send threw, leaving batch for retry: " + e);
							return; // Do not flush -- every dequeue rolls back on session dispose.
						}

						if (result == SendResult.Delivered || result == SendResult.PoisonDrop)
						{
							session.Flush();
							_spoolBytes = Math.Max(0, _spoolBytes - (batchBytes + skippedBytes));
							PersistSpoolBytesLocked(_spoolBytes);
						}
						// RetryableFailure: no flush, so the whole batch rolls back.
					}
				}
				finally
				{
					_sync.Release();
				}
			}
			catch (Exception e)
			{
				Debug.WriteLine("EventSpool.ProcessBatch failed: " + e);
			}
		}

		/// <summary>
		/// Empties the spool entirely (used on consent revocation), removing the event data from
		/// disk -- not merely marking it consumed. Never throws.
		/// </summary>
		/// <remarks>
		/// Implemented as dispose + delete-the-directory + reopen rather than either alternative,
		/// both tried empirically (2026-07, DiskQueue 1.7.2): a dequeue-and-flush loop leaves the
		/// purged events' bytes sitting in the data files (DiskQueue does not trim consumed
		/// entries), which is not acceptable for a CONSENT purge; and DiskQueue's own
		/// <c>HardDelete(reset: true)</c> leaves the live instance's
		/// <c>EstimatedCountOfItemsInQueue</c> stale, so the spool keeps reporting the purged
		/// events as present -- consistent with its "not thread safe ... or safe in any other
		/// way" warning. Between the dispose and the reopen the cross-process exclusive lock is
		/// briefly released; if another process steals it in that window the reopen fails, this
		/// spool degrades to a no-op (every method here already tolerates that), and the purge
		/// itself has still succeeded -- the data is gone, which is the property that matters.
		/// </remarks>
		public void Purge()
		{
			try
			{
				_sync.Wait();
				try
				{
					if (_disposed)
						return;

					try
					{
						_queue?.Dispose();
					}
					finally
					{
						_queue = null;
					}

					if (Directory.Exists(_spoolDirectory))
						Directory.Delete(_spoolDirectory, true);

					_queue = PersistentQueue.WaitFor(_spoolDirectory, s_lockWaitTimeout);
					_spoolBytes = 0;
				}
				finally
				{
					_sync.Release();
				}
			}
			catch (Exception e)
			{
				Debug.WriteLine("EventSpool.Purge failed: " + e);
			}
		}

		/// <summary>
		/// Releases the underlying DiskQueue's cross-process exclusive lock. Safe to call more
		/// than once.
		/// </summary>
		public void Dispose()
		{
			if (_disposed)
				return;

			try
			{
				_sync.Wait();
				try
				{
					_disposed = true;
					_queue?.Dispose();
				}
				finally
				{
					_sync.Release();
				}
			}
			catch (Exception e)
			{
				Debug.WriteLine("EventSpool.Dispose failed: " + e);
			}
		}
	}
}
