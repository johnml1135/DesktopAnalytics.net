using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DesktopAnalytics;
using NUnit.Framework;

namespace DesktopAnalyticsTests
{
	// NOTE on the "corrupt/partial spool" edge case (power loss mid-write): rather than
	// hand-crafting a truncated transaction-log file matching DiskQueue's private on-disk format
	// (which would couple this suite to internals that may change between DiskQueue versions),
	// test 10 below appends format-agnostic garbage bytes to the transaction log and asserts the
	// spool survives -- exercising DiskQueue's transactional recovery (entries are only
	// discoverable after a matching start/end transaction marker pair) without depending on what
	// the markers look like. Separately, EventSpool.ProcessBatch guards against an individual
	// entry that deserializes badly (e.g. an unreadable/incompatible schema version) by skipping
	// (dropping) it like a poison message rather than wedging the spool -- see EventSpool.cs.
	[TestFixture]
	public class EventSpoolTests
	{
		private string _spoolDir;

		[SetUp]
		public void SetUp()
		{
			_spoolDir = Path.Combine(Path.GetTempPath(), "EventSpoolTests_" + Guid.NewGuid());
		}

		[TearDown]
		public void TearDown()
		{
			// Any EventSpool opened by a test must already be disposed (releasing DiskQueue's
			// exclusive lock) by the time we get here -- each test disposes its spool(s) via
			// `using` before returning.
			if (Directory.Exists(_spoolDir))
			{
				try
				{
					Directory.Delete(_spoolDir, true);
				}
				catch (Exception e)
				{
					// Best-effort cleanup; don't fail the test run over a leftover temp directory.
					Console.WriteLine("EventSpoolTests.TearDown: failed to delete " + _spoolDir + ": " + e);
				}
			}
		}

		private static AnalyticsEvent MakeEvent(string name)
		{
			return AnalyticsEvent.Create("user-1", name);
		}

		private static AnalyticsEvent MakeEventAt(string name, DateTimeOffset time)
		{
			return AnalyticsEvent.Create("user-1", name, time: time);
		}

		// Drains the spool with an always-Delivered batch callback, returning the event names in
		// the order they were handed to the sender.
		private static async Task<List<string>> DrainAllDelivered(EventSpool spool, int maxItems = 100)
		{
			var names = new List<string>();
			await spool.ProcessBatchAsync(maxItems, (batch, ct) =>
			{
				names.AddRange(batch.Select(e => e.EventName));
				return Task.FromResult(SendResult.Delivered);
			});
			return names;
		}

		// Reads every file under the spool directory that can be opened for shared reading.
		// DiskQueue holds its 'lock' file open exclusively while the spool is open; that file
		// only ever contains a process id, never event data, so skipping unopenable files does
		// not weaken any event-content assertion.
		internal static IEnumerable<KeyValuePair<string, string>> ReadableSpoolFiles(string spoolDir)
		{
			foreach (var file in Directory.GetFiles(spoolDir, "*", SearchOption.AllDirectories))
			{
				string content;
				try
				{
					using (var stream = new FileStream(file, FileMode.Open, FileAccess.Read,
						       FileShare.ReadWrite | FileShare.Delete))
					using (var reader = new StreamReader(stream, System.Text.Encoding.UTF8))
						content = reader.ReadToEnd();
				}
				catch (IOException)
				{
					continue; // DiskQueue's exclusively-held lock file.
				}

				yield return new KeyValuePair<string, string>(file, content);
			}
		}

		// 1. RESTART DURABILITY: enqueue N, dispose, reopen a NEW spool on the same directory --
		// all N survive, and a subsequent ProcessBatch (Delivered) empties it.
		[Test]
		public async Task Enqueue_ThenDisposeAndReopen_EventsSurviveAndDrainFully()
		{
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				for (var i = 0; i < 5; i++)
					spool.Enqueue(MakeEvent("Event-" + i));
			} // Disposed here, releasing the exclusive lock.

			using (var reopened = new EventSpool(_spoolDir, 10))
			{
				Assert.AreEqual(5, reopened.ApproximateCount);

				var delivered = await DrainAllDelivered(reopened);

				Assert.AreEqual(5, delivered.Count);
				Assert.AreEqual(0, reopened.ApproximateCount);
			}
		}

		// 2. ProcessBatch where send returns Delivered => queue empty afterward.
		[Test]
		public async Task ProcessBatch_AllDelivered_EmptiesSpool()
		{
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				spool.Enqueue(MakeEvent("A"));
				spool.Enqueue(MakeEvent("B"));

				await spool.ProcessBatchAsync(10, (batch, ct) => Task.FromResult(SendResult.Delivered));

				Assert.AreEqual(0, spool.ApproximateCount);
			}
		}

		// 3. send returns RetryableFailure => the WHOLE batch rolls back (nothing is committed --
		// with one request per batch there is no per-event verdict to split on); a later
		// ProcessBatch delivers all of them, in order.
		[Test]
		public async Task ProcessBatch_RetryableFailure_RollsWholeBatchBackForLaterRetry()
		{
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				spool.Enqueue(MakeEvent("A"));
				spool.Enqueue(MakeEvent("B"));
				spool.Enqueue(MakeEvent("C"));

				var firstPass = new List<string>();
				await spool.ProcessBatchAsync(10, (batch, ct) =>
				{
					firstPass.AddRange(batch.Select(e => e.EventName));
					return Task.FromResult(SendResult.RetryableFailure);
				});

				CollectionAssert.AreEqual(new[] { "A", "B", "C" }, firstPass);
				Assert.AreEqual(3, spool.ApproximateCount,
					"a retryable failure must leave the whole batch in the spool");

				var secondPass = await DrainAllDelivered(spool);

				CollectionAssert.AreEqual(new[] { "A", "B", "C" }, secondPass,
					"the rolled-back batch must be retried in its original order");
				Assert.AreEqual(0, spool.ApproximateCount);
			}
		}

		// 4. CRASH-WINDOW: send records the batch then THROWS => nothing removed; reopening the
		// spool shows the same events still present (at-least-once, no data loss).
		[Test]
		public async Task ProcessBatch_SendThrows_NothingRemovedAndEventsSurviveReopen()
		{
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				spool.Enqueue(MakeEvent("Crash-1"));
				spool.Enqueue(MakeEvent("Crash-2"));

				var recorded = new List<string>();
				Assert.DoesNotThrowAsync(async () => await spool.ProcessBatchAsync(10, (batch, ct) =>
				{
					// Simulate a sender that actually delivered the batch over the network, then
					// crashed/threw before this method could observe success and flush.
					recorded.AddRange(batch.Select(e => e.EventName));
					throw new InvalidOperationException("simulated crash after send, before ack");
				}));

				CollectionAssert.AreEqual(new[] { "Crash-1", "Crash-2" }, recorded);
				Assert.AreEqual(2, spool.ApproximateCount);
			}

			using (var reopened = new EventSpool(_spoolDir, 10))
			{
				Assert.AreEqual(2, reopened.ApproximateCount);

				var delivered = await DrainAllDelivered(reopened);

				CollectionAssert.AreEqual(new[] { "Crash-1", "Crash-2" }, delivered);
			}
		}

		// 5. Poison: send returns PoisonDrop => the batch is removed even though not delivered,
		// so a permanently rejected request cannot wedge the spool.
		[Test]
		public async Task ProcessBatch_PoisonDrop_RemovesBatchEvenThoughNotDelivered()
		{
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				spool.Enqueue(MakeEvent("Poison-1"));
				spool.Enqueue(MakeEvent("Poison-2"));

				await spool.ProcessBatchAsync(10, (batch, ct) => Task.FromResult(SendResult.PoisonDrop));

				Assert.AreEqual(0, spool.ApproximateCount,
					"a poison batch must be dropped, not left to wedge the spool");
			}
		}

		// 6. Bounding: maxItems=3, enqueue 5 => count==3, the 3 NEWEST remain.
		[Test]
		public async Task Enqueue_ExceedingMaxItems_DropsOldestKeepsNewest()
		{
			using (var spool = new EventSpool(_spoolDir, 3))
			{
				for (var i = 0; i < 5; i++)
					spool.Enqueue(MakeEvent("Event-" + i));

				Assert.AreEqual(3, spool.ApproximateCount);

				var remaining = await DrainAllDelivered(spool);

				CollectionAssert.AreEqual(new[] { "Event-2", "Event-3", "Event-4" }, remaining);
			}
		}

		// 6a. Degenerate cap: maxItems == 0 means even the event just enqueued is evicted (see the
		// comment on EnforceCapLocked describing this degenerate case). Enqueue itself still
		// reports success (the write genuinely happened; cap enforcement is a separate step), but
		// the spool ends up empty.
		[Test]
		public void Enqueue_MaxItemsZero_ImmediatelyEvictsTheJustEnqueuedEvent()
		{
			using (var spool = new EventSpool(_spoolDir, 0))
			{
				Assert.IsTrue(spool.Enqueue(MakeEvent("A")),
					"Enqueue reports the write itself succeeded, independent of cap enforcement");
				Assert.AreEqual(0, spool.ApproximateCount,
					"maxItems == 0 must evict even the event that was just enqueued");
			}
		}

		// 6b. Boundary: exactly maxItems enqueued => nothing dropped; one more => the oldest is
		// dropped and the count stays pinned at the cap.
		[Test]
		public async Task Enqueue_ExactlyAtMaxItems_NoneDropped_ThenOneOver_DropsOldestStaysAtCap()
		{
			const int maxItems = 5;
			using (var spool = new EventSpool(_spoolDir, maxItems))
			{
				for (var i = 0; i < maxItems; i++)
					spool.Enqueue(MakeEvent("Event-" + i));

				Assert.AreEqual(maxItems, spool.ApproximateCount,
					"exactly maxItems events must all be retained");

				spool.Enqueue(MakeEvent("OneMore"));

				Assert.AreEqual(maxItems, spool.ApproximateCount, "count must stay pinned at the cap");

				var remaining = await DrainAllDelivered(spool, maxItems + 5);
				CollectionAssert.AreEqual(
					new[] { "Event-1", "Event-2", "Event-3", "Event-4", "OneMore" }, remaining,
					"the oldest event must be dropped to make room for the one over the cap");
			}
		}

		// ---- AGE-BASED RETENTION (TrimExpired) --------------------------------------------------

		// An event older than the max age must be dropped.
		[Test]
		public void TrimExpired_EventOlderThanMaxAge_IsDropped()
		{
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				var now = DateTimeOffset.UtcNow;
				spool.Enqueue(MakeEventAt("Old", now - TimeSpan.FromDays(61)));

				spool.TrimExpired(TimeSpan.FromDays(60), now);

				Assert.AreEqual(0, spool.ApproximateCount,
					"an event older than the max age must be dropped");
			}
		}

		// An event within the max age must be retained.
		[Test]
		public void TrimExpired_EventWithinMaxAge_IsRetained()
		{
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				var now = DateTimeOffset.UtcNow;
				spool.Enqueue(MakeEventAt("Recent", now - TimeSpan.FromDays(1)));

				spool.TrimExpired(TimeSpan.FromDays(60), now);

				Assert.AreEqual(1, spool.ApproximateCount,
					"an event within the max age must be retained");
			}
		}

		// A mix of expired and fresh events: only the expired PREFIX is dropped (FIFO order), and
		// the rest survive in their original order.
		[Test]
		public async Task TrimExpired_MixOfExpiredAndFreshEvents_DropsOnlyExpiredPrefixKeepsRestInOrder()
		{
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				var now = DateTimeOffset.UtcNow;
				spool.Enqueue(MakeEventAt("Expired-1", now - TimeSpan.FromDays(90)));
				spool.Enqueue(MakeEventAt("Expired-2", now - TimeSpan.FromDays(70)));
				spool.Enqueue(MakeEventAt("Fresh-1", now - TimeSpan.FromDays(10)));
				spool.Enqueue(MakeEventAt("Fresh-2", now - TimeSpan.FromDays(1)));

				spool.TrimExpired(TimeSpan.FromDays(60), now);

				Assert.AreEqual(2, spool.ApproximateCount);
				var remaining = await DrainAllDelivered(spool);
				CollectionAssert.AreEqual(new[] { "Fresh-1", "Fresh-2" }, remaining,
					"only the expired prefix must be dropped; the rest must survive in order");
			}
		}

		// TrimExpired on an empty spool is a no-op that must not throw.
		[Test]
		public void TrimExpired_EmptySpool_IsNoOpAndDoesNotThrow()
		{
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				Assert.DoesNotThrow(() => spool.TrimExpired(TimeSpan.FromDays(60), DateTimeOffset.UtcNow));
				Assert.AreEqual(0, spool.ApproximateCount);
			}
		}

		// ---- DISK I/O FAILURE (degrade gracefully, never throw) ---------------------------------

		// A "disk full"/inaccessible-file analog: make the spool's own data file read-only out from
		// under it (a real, DiskQueue-external way to force a write failure without hand-rolling a
		// mock of DiskQueue itself) and confirm Enqueue reports the failure via its return value --
		// never by throwing -- and does not corrupt the spool's state.
		[Test]
		public void Enqueue_DataFileMadeReadOnly_ReturnsFalseAndNeverThrowsAndLeavesSpoolConsistent()
		{
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				Assert.IsTrue(spool.Enqueue(MakeEvent("Seed")), "seed event to ensure the data file exists");
				Assert.AreEqual(1, spool.ApproximateCount);

				var dataFile = Path.Combine(_spoolDir, "data.0");
				Assert.IsTrue(File.Exists(dataFile), "expected DiskQueue's data file at " + dataFile);
				File.SetAttributes(dataFile, FileAttributes.ReadOnly);
				try
				{
					bool result = true;
					Assert.DoesNotThrow(() => result = spool.Enqueue(MakeEvent("ShouldFail")),
						"a write failure (disk full/inaccessible analog) must never throw out of Enqueue");
					Assert.IsFalse(result,
						"a write failure must be reported via Enqueue's return value");
					Assert.AreEqual(1, spool.ApproximateCount,
						"a failed write must not corrupt the count of what is actually spooled");
				}
				finally
				{
					File.SetAttributes(dataFile, FileAttributes.Normal);
				}
			}
		}

		// Same idea for the read/drain side: lock the data file exclusively (simulating it being
		// momentarily inaccessible) while ProcessBatchAsync is draining. The dequeue must fail
		// internally, the method must not throw, and -- once the lock is released -- the event must
		// still be present and retrievable (nothing was lost by the transient failure).
		[Test]
		public async Task ProcessBatch_DataFileTransientlyLocked_DegradesGracefullyAndEventSurvives()
		{
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				spool.Enqueue(MakeEvent("A"));

				var dataFile = Path.Combine(_spoolDir, "data.0");
				Assert.IsTrue(File.Exists(dataFile), "expected DiskQueue's data file at " + dataFile);

				var sentDuringLock = new List<string>();
				using (new FileStream(dataFile, FileMode.Open, FileAccess.Read, FileShare.None))
				{
					Assert.DoesNotThrowAsync(async () => await spool.ProcessBatchAsync(10, (batch, ct) =>
					{
						sentDuringLock.AddRange(batch.Select(e => e.EventName));
						return Task.FromResult(SendResult.Delivered);
					}));
				}

				Assert.AreEqual(0, sentDuringLock.Count,
					"the transiently locked file must prevent the dequeue from succeeding");

				var delivered = await DrainAllDelivered(spool);
				CollectionAssert.AreEqual(new[] { "A" }, delivered,
					"the event must survive a transient read failure and still be retrievable afterward");
			}
		}

		// 7. Purge empties a non-empty spool.
		[Test]
		public async Task Purge_EmptiesNonEmptySpool()
		{
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				for (var i = 0; i < 5; i++)
					spool.Enqueue(MakeEvent("Event-" + i));

				Assert.AreEqual(5, spool.ApproximateCount);

				spool.Purge();

				Assert.AreEqual(0, spool.ApproximateCount);

				// A drain after Purge should find nothing to send.
				var delivered = await DrainAllDelivered(spool);
				Assert.AreEqual(0, delivered.Count);
			}
		}

		// 7b. Purge leaves the spool fully usable: new events can be enqueued in the same
		// session, only they survive a dispose/reopen, and the purged events' bytes are gone
		// from the on-disk files once the spool is closed (the privacy property of a consent
		// purge -- DiskQueue trims consumed entries rather than retaining them).
		[Test]
		public async Task Purge_ThenEnqueue_SpoolRemainsUsableAndPurgedBytesAreGoneFromDisk()
		{
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				spool.Enqueue(MakeEvent("PrePurgeSecret"));
				Assert.AreEqual(1, spool.ApproximateCount);

				spool.Purge();
				Assert.AreEqual(0, spool.ApproximateCount);

				spool.Enqueue(MakeEvent("PostPurge"));
				Assert.AreEqual(1, spool.ApproximateCount);
			}

			foreach (var file in ReadableSpoolFiles(_spoolDir))
			{
				StringAssert.DoesNotContain("PrePurgeSecret", file.Value,
					"a consent purge must remove event data from disk, not just mark it consumed: " + file.Key);
			}

			using (var reopened = new EventSpool(_spoolDir, 10))
			{
				var delivered = await DrainAllDelivered(reopened);
				CollectionAssert.AreEqual(new[] { "PostPurge" }, delivered);
			}
		}

		// 8. Concurrent enqueue from multiple threads (single process): 4 threads x 25 events =>
		// all 100 persisted. Dedicated Threads (not Task.Run) so the intended parallelism is
		// explicit rather than at the mercy of thread-pool scheduling.
		[Test]
		public async Task Enqueue_FromMultipleThreadsConcurrently_AllEventsPersisted()
		{
			const int threadCount = 4;
			const int perThread = 25;

			using (var spool = new EventSpool(_spoolDir, threadCount * perThread))
			{
				var threads = new Thread[threadCount];
				for (var t = 0; t < threadCount; t++)
				{
					var threadIndex = t;
					threads[t] = new Thread(() =>
					{
						for (var i = 0; i < perThread; i++)
							spool.Enqueue(MakeEvent($"T{threadIndex}-{i}"));
					});
				}

				foreach (var thread in threads)
					thread.Start();
				foreach (var thread in threads)
					thread.Join();

				Assert.AreEqual(threadCount * perThread, spool.ApproximateCount);

				// Confirm every single one is actually retrievable, not just counted.
				var seen = new HashSet<string>(
					await DrainAllDelivered(spool, threadCount * perThread));
				Assert.AreEqual(threadCount * perThread, seen.Count);
			}
		}

		// 8a. Per-event byte cap: an event whose serialized form exceeds maxItemBytes is refused
		// (Enqueue returns false) and never spooled, while a normal event on the same spool is
		// accepted -- and Enqueue's return value distinguishes the two.
		[Test]
		public void Enqueue_EventLargerThanMaxItemBytes_IsRefusedWhileNormalEventIsAccepted()
		{
			using (var spool = new EventSpool(_spoolDir, 10, maxItemBytes: 500))
			{
				var oversized = AnalyticsEvent.Create("user-1", "Huge",
					new Segment.Serialization.JsonObject
					{
						{ "Stack Trace", new string('x', 2000) }
					});

				Assert.IsFalse(spool.Enqueue(oversized), "an event over the byte cap must be refused");
				Assert.AreEqual(0, spool.ApproximateCount);

				Assert.IsTrue(spool.Enqueue(MakeEvent("Normal")), "a normal event must still be accepted");
				Assert.AreEqual(1, spool.ApproximateCount);
			}
		}

		// 8a2. Byte-based spool cap: enqueuing past maxSpoolBytes drops the oldest event(s),
		// exactly like the item cap.
		[Test]
		public async Task Enqueue_ExceedingMaxSpoolBytes_DropsOldestKeepsNewest()
		{
			var events = new[] { MakeEvent("Event-A"), MakeEvent("Event-B"), MakeEvent("Event-C"), MakeEvent("Event-D") };
			// Cap sized (from the real serialized lengths) to hold the first three but not all four.
			long cap = events[0].ToBytes().Length + events[1].ToBytes().Length + events[2].ToBytes().Length;

			using (var spool = new EventSpool(_spoolDir, 100, maxSpoolBytes: cap))
			{
				foreach (var evt in events)
					spool.Enqueue(evt);

				var remaining = await DrainAllDelivered(spool);

				CollectionAssert.AreEqual(new[] { "Event-B", "Event-C", "Event-D" }, remaining,
					"the byte cap must drop the OLDEST event to make room");
			}
		}

		// 8a3. The byte accounting behind the cap survives a restart -- normally read back from the
		// total persisted alongside the spool (see ResolveExistingSpoolBytes), falling back to
		// re-measuring the live entries if that is missing or untrustworthy -- so a reopened spool
		// enforces the cap correctly either way.
		[Test]
		public void ApproximateBytes_SurvivesDisposeAndReopen()
		{
			long expectedBytes;
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				spool.Enqueue(MakeEvent("Event-0"));
				spool.Enqueue(MakeEvent("Event-1"));
				expectedBytes = spool.ApproximateBytes;
				Assert.Greater(expectedBytes, 0);
			}

			using (var reopened = new EventSpool(_spoolDir, 10))
			{
				Assert.AreEqual(expectedBytes, reopened.ApproximateBytes,
					"the persisted byte total must be restored on open");
				Assert.AreEqual(2, reopened.ApproximateCount, "opening must not consume the entries");
			}
		}

		// 8a3b. If the persisted byte-total sidecar file is missing or corrupt (e.g. this session
		// crashed before persisting, or the file predates this feature), the spool must fall back to
		// re-measuring from the live entries rather than trusting a stale/absent value -- the cap
		// must still be enforced correctly on the next restart.
		[Test]
		public void ApproximateBytes_MissingPersistedTotal_FallsBackToMeasuring()
		{
			long expectedBytes;
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				spool.Enqueue(MakeEvent("Event-0"));
				spool.Enqueue(MakeEvent("Event-1"));
				expectedBytes = spool.ApproximateBytes;
			}

			File.Delete(Path.Combine(_spoolDir, "spool-bytes.txt"));

			using (var reopened = new EventSpool(_spoolDir, 10))
			{
				Assert.AreEqual(expectedBytes, reopened.ApproximateBytes,
					"a missing persisted total must fall back to re-measuring the live entries");
				Assert.AreEqual(2, reopened.ApproximateCount, "measuring must not consume the entries");
			}
		}

		// 8a4. ProcessBatch byte budget: the gather stops once the batch reaches maxBytes, but
		// a single event over the whole budget still goes (forward progress is guaranteed).
		[Test]
		public async Task ProcessBatch_ByteBudget_BoundsBatchButNeverStarves()
		{
			var events = new[] { MakeEvent("Event-0"), MakeEvent("Event-1"), MakeEvent("Event-2"),
				MakeEvent("Event-3"), MakeEvent("Event-4") };
			// A budget the first two events fit under but the third pushes past.
			long budget = events[0].ToBytes().Length + events[1].ToBytes().Length + 1;

			using (var spool = new EventSpool(_spoolDir, 10))
			{
				foreach (var evt in events)
					spool.Enqueue(evt);

				var firstBatch = new List<string>();
				await spool.ProcessBatchAsync(10, (batch, ct) =>
				{
					firstBatch.AddRange(batch.Select(e => e.EventName));
					return Task.FromResult(SendResult.Delivered);
				}, budget);

				CollectionAssert.AreEqual(new[] { "Event-0", "Event-1", "Event-2" }, firstBatch,
					"the budget is checked AFTER each gathered event, so the event that crosses it is still included");
				Assert.AreEqual(2, spool.ApproximateCount);

				// A budget smaller than any single event must still make progress, one event per call.
				var secondBatch = new List<string>();
				await spool.ProcessBatchAsync(10, (batch, ct) =>
				{
					secondBatch.AddRange(batch.Select(e => e.EventName));
					return Task.FromResult(SendResult.Delivered);
				}, maxBytes: 1);

				CollectionAssert.AreEqual(new[] { "Event-3" }, secondBatch);
				Assert.AreEqual(1, spool.ApproximateCount);
			}
		}

		// 8b. ProcessBatch honors maxItems: with more events spooled than the batch size, exactly
		// maxItems are gathered per call and the rest stay put for the next call.
		[Test]
		public async Task ProcessBatch_MoreEventsThanMaxItems_GathersExactlyMaxItemsPerCall()
		{
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				for (var i = 0; i < 5; i++)
					spool.Enqueue(MakeEvent("Event-" + i));

				var firstBatch = await DrainAllDelivered(spool, maxItems: 2);

				CollectionAssert.AreEqual(new[] { "Event-0", "Event-1" }, firstBatch);
				Assert.AreEqual(3, spool.ApproximateCount);

				var secondBatch = await DrainAllDelivered(spool, maxItems: 2);

				CollectionAssert.AreEqual(new[] { "Event-2", "Event-3" }, secondBatch);
				Assert.AreEqual(1, spool.ApproximateCount);
			}
		}

		// 8c. CANCELLATION: a pre-canceled token means the batch is never gathered or sent, the
		// spool is untouched, and nothing throws -- and the send callback receives the SAME token
		// that was passed in, so an in-flight send can participate in cancellation.
		[Test]
		public async Task ProcessBatch_CancellationToken_IsHonoredAndPassedThroughToSend()
		{
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				spool.Enqueue(MakeEvent("A"));

				using (var cts = new CancellationTokenSource())
				{
					cts.Cancel();
					var sendCalled = false;
					Assert.DoesNotThrowAsync(async () => await spool.ProcessBatchAsync(10, (batch, ct) =>
					{
						sendCalled = true;
						return Task.FromResult(SendResult.Delivered);
					}, cancellationToken: cts.Token));

					Assert.IsFalse(sendCalled, "a pre-canceled token must short-circuit before the send");
					Assert.AreEqual(1, spool.ApproximateCount, "the event must remain spooled");
				}

				using (var cts = new CancellationTokenSource())
				{
					CancellationToken observed = default;
					await spool.ProcessBatchAsync(10, (batch, ct) =>
					{
						observed = ct;
						return Task.FromResult(SendResult.Delivered);
					}, cancellationToken: cts.Token);

					Assert.AreEqual(cts.Token, observed,
						"the send callback must receive the caller's token so it can participate in cancellation");
					Assert.AreEqual(0, spool.ApproximateCount);
				}
			}
		}

		// 9. Second EventSpool opened on the SAME directory while the first is still open
		// fails/throws, proving the cross-process exclusive lock (DiskQueue.PersistentQueue.WaitFor
		// with a short internal timeout) does not silently allow shared access.
		[Test]
		public void Constructor_SecondSpoolOnSameDirectoryWhileFirstOpen_Throws()
		{
			using (new EventSpool(_spoolDir, 10))
			{
				// Assert.Catch (unlike Assert.Throws<Exception>, which requires an *exact* type
				// match) accepts any exception assignable to Exception -- we don't want this test
				// coupled to DiskQueue's specific exception type (currently TimeoutException) for
				// a lock-acquisition failure.
				Assert.Catch<Exception>(() =>
				{
					using (new EventSpool(_spoolDir, 10))
					{
					}
				});
			}
		}

		// 10. CORRUPTION, torn data-file write: garbage appended to a data file (bytes beyond the
		// extents any committed transaction references) must not prevent reopening the spool, and
		// events from committed transactions must still be readable. Appending garbage -- rather
		// than hand-crafting file contents -- keeps this test independent of DiskQueue's private
		// on-disk format (see the NOTE at the top of this fixture).
		[Test]
		public async Task Constructor_GarbageAppendedToDataFile_SpoolReopensAndCommittedEventsSurvive()
		{
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				spool.Enqueue(MakeEvent("Committed-1"));
				spool.Enqueue(MakeEvent("Committed-2"));
			} // Disposed: both enqueues are fully committed transactions on disk.

			AppendGarbageTo(FindSpoolFile("data.0"));

			using (var reopened = new EventSpool(_spoolDir, 10))
			{
				var delivered = await DrainAllDelivered(reopened);
				CollectionAssert.AreEqual(new[] { "Committed-1", "Committed-2" }, delivered);

				// And the recovered spool must still accept new work.
				reopened.Enqueue(MakeEvent("PostRecovery"));
				Assert.AreEqual(1, reopened.ApproximateCount);
			}
		}

		// 10b. CORRUPTION, garbage in the transaction log: DiskQueue deliberately refuses to open
		// a log with unrecognized trailing bytes (it recovers a TRUNCATED log -- the realistic
		// power-loss shape, where entries simply end mid-transaction -- but treats garbage after a
		// committed entry as a conflict). Pin that fail-fast behavior here: the constructor throws
		// rather than silently serving corrupt data. At the client level this is already handled:
		// MixpanelClient.Initialize catches the throw and degrades to a non-durable no-op client
		// rather than crashing the host (see MixpanelClient's class remarks).
		[Test]
		public void Constructor_GarbageAppendedToTransactionLog_FailsFastRatherThanServingCorruptData()
		{
			using (var spool = new EventSpool(_spoolDir, 10))
			{
				spool.Enqueue(MakeEvent("Committed-1"));
			}

			AppendGarbageTo(FindSpoolFile("transaction.log"));

			Assert.Catch<Exception>(() =>
			{
				using (new EventSpool(_spoolDir, 10))
				{
				}
			});
		}

		private string FindSpoolFile(string fileName)
		{
			var path = Path.Combine(_spoolDir, fileName);
			Assert.IsTrue(File.Exists(path),
				"expected DiskQueue file at " + path + " -- if DiskQueue renamed it, update this test");
			return path;
		}

		private static void AppendGarbageTo(string path)
		{
			var garbage = new byte[257];
			new Random(12345).NextBytes(garbage);
			using (var stream = new FileStream(path, FileMode.Append, FileAccess.Write))
				stream.Write(garbage, 0, garbage.Length);
		}
	}
}
