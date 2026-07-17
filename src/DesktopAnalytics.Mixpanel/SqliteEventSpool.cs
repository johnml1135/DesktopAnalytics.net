using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace DesktopAnalytics
{
	/// <summary>
	/// SQLite-backed <see cref="IEventSpool"/>. Same contract as the (now removed) DiskQueue-backed
	/// <c>EventSpool</c>, but the storage model removes -- rather than works around -- the two
	/// structural problems that one had.
	/// </summary>
	/// <remarks>
	/// <para><b>Why this exists.</b> As originally written, <c>EventSpool</c> held its global
	/// <c>_sync</c> semaphore across the network send, so every concurrent <c>Track()</c> waited out
	/// the whole send -- measured at ~3s behind a 3s send, and up to ~45s in production against a
	/// stalled connection (15s HttpClient timeout x 3 pipeline attempts). That was a UI freeze on
	/// exactly the "bad cafe Wi-Fi" path this whole feature is for.</para>
	/// <para><b>Note:</b> that stall was not inherent to DiskQueue. Probed 2026-07-16: DiskQueue
	/// happily enqueues (~8ms) while an uncommitted dequeue session is open, and a second session
	/// dequeues a DISTINCT item rather than blocking or double-serving. So dropping
	/// <c>EventSpool</c>'s global lock would have fixed the stall without changing engines. The
	/// case for SQLite rests on the OTHER problems below, not on the stall alone.</para>
	/// <para>SQLite needs no such thing. <see cref="ProcessBatchAsync"/> claims a batch under a
	/// short lock, <b>releases the lock</b>, sends with nothing held, then removes the delivered
	/// rows under a second short lock. At-least-once falls out for free: a crash between the send
	/// and the removal replays the batch, which Mixpanel deduplicates on
	/// <see cref="AnalyticsEvent.InsertId"/> -- the semantics the design already depends on.
	/// Because the lock is never held across an await, a plain <c>lock</c> suffices here where
	/// <c>EventSpool</c> needed a <c>SemaphoreSlim</c>.</para>
	/// <para><b>What it deletes.</b> The byte-total sidecar file, its staleness heuristic, and its
	/// re-measure fallback all collapse into <c>SUM(len)</c>. Age retention collapses into one
	/// indexed <c>DELETE</c>. Cap eviction collapses into one <c>DELETE</c>. Purge becomes
	/// <c>DELETE</c> + <c>VACUUM</c> instead of dispose + delete-the-directory + reopen.</para>
	/// <para><b>Multi-process.</b> WAL gives real concurrent readers/writers, so a second
	/// FieldWorks process spools and drains normally. The old DiskQueue-backed spool could not even
	/// open (exclusive lock) and silently degraded to a no-op, losing its events entirely. A short
	/// lease on claimed rows keeps two processes from uploading the same batch; the lease expires
	/// on its own, so a process that dies mid-send does not strand its batch.</para>
	/// </remarks>
	internal class SqliteEventSpool : IEventSpool
	{
		// How long a claimed batch stays invisible to other drains. Long enough to cover a slow
		// send, short enough that a crashed process's batch is retried promptly.
		private static readonly TimeSpan s_leaseDuration = TimeSpan.FromMinutes(2);

		// How long SQLite waits on a lock held by another process before returning SQLITE_BUSY.
		private const int kBusyTimeoutMs = 2000;

		private readonly int _maxItems;
		private readonly int _maxItemBytes;
		private readonly long _maxSpoolBytes;
		private readonly string _spoolDirectory;
		private readonly string _databasePath;
		private readonly TimeProvider _timeProvider;

		// Guards _connection. Only ever held for short, purely-local database work -- NEVER across
		// the network send (see the class remarks). A monitor lock is therefore sufficient.
		private readonly object _sync = new object();
		private SqliteConnection _connection;
		private bool _disposed;

		/// <inheritdoc/>
		public event Action ItemDroppedByCap;

		/// <param name="spoolDirectory">Directory to hold the spool database. Callers normally get
		/// this from <see cref="GetDefaultSpoolPath"/>; tests inject a temp directory.</param>
		/// <param name="maxItems">Maximum number of events retained; enqueuing beyond this drops
		/// the oldest first.</param>
		/// <param name="maxItemBytes">Maximum serialized size of a single event; larger events are
		/// refused at enqueue rather than spooled.</param>
		/// <param name="maxSpoolBytes">Maximum total serialized size of all retained events;
		/// enqueuing beyond this drops the oldest first.</param>
		/// <param name="timeProvider">Clock for lease expiry. Injected so tests stay deterministic.</param>
		/// <exception cref="Exception">Propagates a failure to create/open the database. This is
		/// deliberately not swallowed: it is a startup-time condition the caller needs to know
		/// about.</exception>
		public SqliteEventSpool(string spoolDirectory, int maxItems, int maxItemBytes = int.MaxValue,
			long maxSpoolBytes = long.MaxValue, TimeProvider timeProvider = null)
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
			_databasePath = Path.Combine(spoolDirectory, "spool.db");
			_timeProvider = timeProvider ?? TimeProvider.System;

			Directory.CreateDirectory(spoolDirectory);
			_connection = OpenConnection(_databasePath);
			InitializeSchema(_connection);
		}

		private static SqliteConnection OpenConnection(string databasePath)
		{
			// Pooling off: this class holds its single connection open for its whole lifetime, so
			// pooling buys nothing -- and it would keep the file handle alive after Dispose,
			// blocking Purge's VACUUM and the temp-directory cleanup in tests.
			var connectionString = new SqliteConnectionStringBuilder
			{
				DataSource = databasePath,
				Pooling = false
			}.ToString();

			var connection = new SqliteConnection(connectionString);
			connection.Open();

			// WAL: concurrent readers and a writer, across processes -- the property DiskQueue's
			// exclusive lock cannot give us.
			// synchronous=NORMAL: no fsync per commit. A power loss (not a process crash -- WAL
			// still recovers those) can lose the last few events. That is the right trade for
			// analytics: the alternative fsyncs on every Track(), on the UI thread.
			Execute(connection, "PRAGMA journal_mode=WAL;");
			Execute(connection, "PRAGMA synchronous=NORMAL;");
			Execute(connection, "PRAGMA busy_timeout=" + kBusyTimeoutMs + ";");
			return connection;
		}

		private static void InitializeSchema(SqliteConnection connection)
		{
			Execute(connection,
				// len is stored rather than computed so the cap queries below are index-only scans
				// and never have to read the payload blobs.
				"CREATE TABLE IF NOT EXISTS events (" +
				"  id INTEGER PRIMARY KEY AUTOINCREMENT," +
				"  time_unix_ms INTEGER NOT NULL," +
				"  lease_until_unix_ms INTEGER NOT NULL DEFAULT 0," +
				"  len INTEGER NOT NULL," +
				"  payload BLOB NOT NULL);");
			Execute(connection, "CREATE INDEX IF NOT EXISTS ix_events_time ON events(time_unix_ms);");
			Execute(connection,
				"CREATE INDEX IF NOT EXISTS ix_events_lease ON events(lease_until_unix_ms, id);");
		}

		private static void Execute(SqliteConnection connection, string sql)
		{
			using (var cmd = connection.CreateCommand())
			{
				cmd.CommandText = sql;
				cmd.ExecuteNonQuery();
			}
		}

		// Number of leading bytes of the SHA-256 hash of the API key used to build the spool
		// directory name. Just needs to be stable and distinguish keys from each other -- it is
		// not a security boundary -- so a short prefix is plenty.
		private const int kApiKeyHashBytes = 8;

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

		/// <inheritdoc/>
		public int ApproximateCount
		{
			get
			{
				try
				{
					lock (_sync)
					{
						if (_disposed || _connection == null)
							return 0;
						return (int)ScalarLocked("SELECT COUNT(*) FROM events;");
					}
				}
				catch (Exception e)
				{
					Debug.WriteLine("SqliteEventSpool.ApproximateCount failed: " + e);
					return 0;
				}
			}
		}

		/// <inheritdoc/>
		public long ApproximateBytes
		{
			get
			{
				try
				{
					lock (_sync)
					{
						if (_disposed || _connection == null)
							return 0;
						return ScalarLocked("SELECT COALESCE(SUM(len), 0) FROM events;");
					}
				}
				catch (Exception e)
				{
					Debug.WriteLine("SqliteEventSpool.ApproximateBytes failed: " + e);
					return 0;
				}
			}
		}

		// Caller must hold _sync.
		private long ScalarLocked(string sql)
		{
			using (var cmd = _connection.CreateCommand())
			{
				cmd.CommandText = sql;
				return Convert.ToInt64(cmd.ExecuteScalar());
			}
		}

		/// <inheritdoc/>
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
					Debug.WriteLine("SqliteEventSpool.Enqueue: failed to serialize event, dropping: " + e);
					return false;
				}

				if (bytes.Length > _maxItemBytes)
				{
					// Refused up front rather than spooled: an event over the transport's per-event
					// limit can never be delivered, and one large enough to time out the request
					// would classify as RETRYABLE (indistinguishable from being offline) and wedge
					// the head of the queue forever.
					Debug.WriteLine("SqliteEventSpool.Enqueue: event of " + bytes.Length +
						" bytes exceeds the " + _maxItemBytes + "-byte cap, dropping: " + evt.EventName);
					return false;
				}

				int dropped;
				lock (_sync)
				{
					if (_disposed || _connection == null)
						return false;

					using (var cmd = _connection.CreateCommand())
					{
						cmd.CommandText =
							"INSERT INTO events (time_unix_ms, lease_until_unix_ms, len, payload) " +
							"VALUES (@time, 0, @len, @payload);";
						cmd.Parameters.AddWithValue("@time", evt.Time.ToUnixTimeMilliseconds());
						cmd.Parameters.AddWithValue("@len", bytes.Length);
						cmd.Parameters.AddWithValue("@payload", bytes);
						cmd.ExecuteNonQuery();
					}

					dropped = EnforceCapsLocked();
				}

				// Raised outside the lock: handlers are the caller's code, and nothing here needs
				// the lock held while they run.
				RaiseDropped(dropped);
				return true;
			}
			catch (Exception e)
			{
				Debug.WriteLine("SqliteEventSpool.Enqueue failed: " + e);
				return false;
			}
		}

		/// <inheritdoc/>
		public Task<bool> EnqueueAsync(AnalyticsEvent evt) => Task.FromResult(Enqueue(evt));

		private void RaiseDropped(int count)
		{
			for (var i = 0; i < count; i++)
			{
				try
				{
					ItemDroppedByCap?.Invoke();
				}
				catch (Exception e)
				{
					Debug.WriteLine("SqliteEventSpool: ItemDroppedByCap handler threw: " + e);
				}
			}
		}

		// Caller must hold _sync. Brings the spool back within BOTH caps, dropping oldest first,
		// and returns how many events were dropped. Two DELETEs, no loop -- contrast the old
		// DiskQueue-backed EventSpool.EnforceCapLocked, which dequeued one item at a time.
		private int EnforceCapsLocked()
		{
			var dropped = 0;
			try
			{
				// Item cap. Also covers the degenerate maxItems == 0 case, where the event just
				// enqueued is itself evicted.
				using (var cmd = _connection.CreateCommand())
				{
					cmd.CommandText =
						"DELETE FROM events WHERE id IN (" +
						"  SELECT id FROM events ORDER BY id ASC" +
						"  LIMIT MAX(0, (SELECT COUNT(*) FROM events) - @maxItems));";
					cmd.Parameters.AddWithValue("@maxItems", _maxItems);
					dropped += cmd.ExecuteNonQuery();
				}

				// Byte cap. The window function totals bytes newest-first; any row whose inclusion
				// pushes that running total past the cap is older than what we can afford to keep,
				// so it goes.
				if (_maxSpoolBytes < long.MaxValue)
				{
					using (var cmd = _connection.CreateCommand())
					{
						cmd.CommandText =
							"DELETE FROM events WHERE id IN (" +
							"  SELECT id FROM (" +
							"    SELECT id, SUM(len) OVER (ORDER BY id DESC) AS running FROM events" +
							"  ) WHERE running > @maxBytes);";
						cmd.Parameters.AddWithValue("@maxBytes", _maxSpoolBytes);
						dropped += cmd.ExecuteNonQuery();
					}
				}
			}
			catch (Exception e)
			{
				Debug.WriteLine("SqliteEventSpool.EnforceCaps failed: " + e);
			}

			return dropped;
		}

		/// <inheritdoc/>
		/// <remarks>
		/// The lock is taken twice -- once to claim, once to resolve -- and is NOT held across
		/// <paramref name="send"/>. See the class remarks for why that is the entire point of this
		/// implementation.
		/// </remarks>
		public async Task ProcessBatchAsync(int maxItems,
			Func<IReadOnlyList<AnalyticsEvent>, CancellationToken, Task<SendResult>> send,
			long maxBytes = long.MaxValue, CancellationToken cancellationToken = default)
		{
			if (send == null || maxItems <= 0 || maxBytes <= 0)
				return;

			try
			{
				var claim = ClaimBatch(maxItems, maxBytes);
				if (claim.Ids.Count == 0)
					return;

				if (claim.Events.Count == 0)
				{
					// Everything claimed was undeserializable (corrupt, or an incompatible schema
					// version). Nothing to send; just remove them so they cannot wedge the spool.
					ResolveClaim(claim.Ids, remove: true);
					return;
				}

				SendResult result;
				try
				{
					// NO LOCK HELD HERE. Track() stays responsive for the whole round trip.
					result = await send(claim.Events, cancellationToken).ConfigureAwait(false);
				}
				catch (Exception e)
				{
					Debug.WriteLine("SqliteEventSpool.ProcessBatch: send threw, leaving batch for retry: " + e);
					ResolveClaim(claim.Ids, remove: false);
					return;
				}

				// Delivered/PoisonDrop: the batch is finished with either way, so remove it.
				// RetryableFailure: release the lease so the next drain picks it up, in order.
				ResolveClaim(claim.Ids,
					remove: result == SendResult.Delivered || result == SendResult.PoisonDrop,
					undeserializableIds: claim.UndeserializableIds);
			}
			catch (Exception e)
			{
				Debug.WriteLine("SqliteEventSpool.ProcessBatch failed: " + e);
			}
		}

		private class ClaimedBatch
		{
			public readonly List<long> Ids = new List<long>();
			public readonly List<AnalyticsEvent> Events = new List<AnalyticsEvent>();
			// Claimed rows whose payload could not be deserialized. Removed regardless of the
			// batch's verdict -- they can never be delivered.
			public readonly List<long> UndeserializableIds = new List<long>();
		}

		// Short, purely-local transaction: pick the oldest unleased rows within both budgets and
		// lease them so a concurrent drain (this process or another) will not send them too.
		private ClaimedBatch ClaimBatch(int maxItems, long maxBytes)
		{
			var claim = new ClaimedBatch();

			lock (_sync)
			{
				if (_disposed || _connection == null)
					return claim;

				var nowMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
				var leaseUntilMs = nowMs + (long)s_leaseDuration.TotalMilliseconds;

				// IMMEDIATE: this transaction reads and then writes, and a deferred transaction
				// that upgrades can deadlock against another process doing the same (both hold
				// SHARED, both want RESERVED) in a way busy_timeout cannot resolve.
				using (var txn = _connection.BeginTransaction(deferred: false))
				{
					long batchBytes = 0;
					using (var cmd = _connection.CreateCommand())
					{
						cmd.Transaction = txn;
						cmd.CommandText =
							"SELECT id, len, payload FROM events WHERE lease_until_unix_ms <= @now " +
							"ORDER BY id ASC LIMIT @maxItems;";
						cmd.Parameters.AddWithValue("@now", nowMs);
						cmd.Parameters.AddWithValue("@maxItems", maxItems);

						using (var reader = cmd.ExecuteReader())
						{
							while (reader.Read())
							{
								// Budget checked BEFORE adding, against the bytes already gathered,
								// so the first event always goes even if it alone exceeds the
								// budget -- the budget can never starve the queue.
								if (batchBytes >= maxBytes)
									break;

								var id = reader.GetInt64(0);
								var len = reader.GetInt64(1);
								var payload = (byte[])reader.GetValue(2);

								claim.Ids.Add(id);
								batchBytes += len;

								try
								{
									claim.Events.Add(AnalyticsEvent.FromBytes(payload));
								}
								catch (Exception e)
								{
									Debug.WriteLine(
										"SqliteEventSpool.ClaimBatch: failed to deserialize event, dropping: " + e);
									claim.UndeserializableIds.Add(id);
								}
							}
						}
					}

					if (claim.Ids.Count > 0)
					{
						using (var cmd = _connection.CreateCommand())
						{
							cmd.Transaction = txn;
							cmd.CommandText =
								"UPDATE events SET lease_until_unix_ms = @leaseUntil WHERE id IN (" +
								IdList(claim.Ids) + ");";
							cmd.Parameters.AddWithValue("@leaseUntil", leaseUntilMs);
							cmd.ExecuteNonQuery();
						}
					}

					txn.Commit();
				}
			}

			return claim;
		}

		// Short, purely-local transaction closing out a claim: either remove the rows (the batch is
		// finished with) or clear their lease so the next drain retries them in order.
		private void ResolveClaim(List<long> ids, bool remove, List<long> undeserializableIds = null)
		{
			try
			{
				lock (_sync)
				{
					if (_disposed || _connection == null)
						return;

					using (var txn = _connection.BeginTransaction(deferred: false))
					{
						if (remove)
						{
							DeleteByIdLocked(txn, ids);
						}
						else
						{
							// Undeserializable rows are removed even on a retryable verdict: they
							// can never be delivered, so retrying them forever would wedge the
							// head of the queue.
							if (undeserializableIds != null && undeserializableIds.Count > 0)
								DeleteByIdLocked(txn, undeserializableIds);

							var toRelease = new List<long>(ids);
							if (undeserializableIds != null)
								toRelease.RemoveAll(undeserializableIds.Contains);

							if (toRelease.Count > 0)
							{
								using (var cmd = _connection.CreateCommand())
								{
									cmd.Transaction = txn;
									cmd.CommandText =
										"UPDATE events SET lease_until_unix_ms = 0 WHERE id IN (" +
										IdList(toRelease) + ");";
									cmd.ExecuteNonQuery();
								}
							}
						}

						txn.Commit();
					}
				}
			}
			catch (Exception e)
			{
				Debug.WriteLine("SqliteEventSpool.ResolveClaim failed: " + e);
			}
		}

		private void DeleteByIdLocked(SqliteTransaction txn, List<long> ids)
		{
			if (ids.Count == 0)
				return;

			using (var cmd = _connection.CreateCommand())
			{
				cmd.Transaction = txn;
				cmd.CommandText = "DELETE FROM events WHERE id IN (" + IdList(ids) + ");";
				cmd.ExecuteNonQuery();
			}
		}

		// Ids are database-generated integers we just read back, never caller input, so inlining
		// them cannot be an injection vector -- and it avoids building N parameters per batch.
		private static string IdList(List<long> ids) => string.Join(",", ids);

		/// <inheritdoc/>
		/// <remarks>
		/// One indexed DELETE. Unlike the old DiskQueue-backed EventSpool's TrimExpired, this does
		/// not have to stop at the first non-expired event -- it removes every expired event wherever it sits,
		/// so an out-of-order timestamp cannot shield older events from the retention floor. A
		/// corrupt payload is irrelevant here too: the age lives in a column, so nothing has to be
		/// deserialized to be dated.
		/// </remarks>
		public void TrimExpired(TimeSpan maxAge, DateTimeOffset now)
		{
			try
			{
				var cutoffMs = (now - maxAge).ToUnixTimeMilliseconds();

				lock (_sync)
				{
					if (_disposed || _connection == null)
						return;

					using (var cmd = _connection.CreateCommand())
					{
						cmd.CommandText = "DELETE FROM events WHERE time_unix_ms < @cutoff;";
						cmd.Parameters.AddWithValue("@cutoff", cutoffMs);
						cmd.ExecuteNonQuery();
					}
				}
			}
			catch (Exception e)
			{
				Debug.WriteLine("SqliteEventSpool.TrimExpired failed: " + e);
			}
		}

		/// <inheritdoc/>
		/// <remarks>
		/// DELETE removes the rows; VACUUM rebuilds the database file so the purged bytes are
		/// actually gone rather than lingering as free pages; the WAL checkpoint/truncate does the
		/// same for the write-ahead log. All three matter for a CONSENT purge, where "marked
		/// consumed" is not good enough.
		/// <para>Contrast the old DiskQueue-backed EventSpool's Purge, which had to dispose the
		/// queue, delete the whole directory and reopen -- briefly dropping its cross-process lock,
		/// and degrading to a no-op spool if another process stole it in that window.</para>
		/// </remarks>
		public void Purge()
		{
			try
			{
				lock (_sync)
				{
					if (_disposed || _connection == null)
						return;

					Execute(_connection, "DELETE FROM events;");
					// Must run outside a transaction, and after the DELETE is committed.
					Execute(_connection, "VACUUM;");
					Execute(_connection, "PRAGMA wal_checkpoint(TRUNCATE);");
				}
			}
			catch (Exception e)
			{
				Debug.WriteLine("SqliteEventSpool.Purge failed: " + e);
			}
		}

		/// <summary>Closes the database. Safe to call more than once.</summary>
		public void Dispose()
		{
			try
			{
				lock (_sync)
				{
					if (_disposed)
						return;

					_disposed = true;
					_connection?.Dispose();
					_connection = null;
				}
			}
			catch (Exception e)
			{
				Debug.WriteLine("SqliteEventSpool.Dispose failed: " + e);
			}
		}
	}
}
