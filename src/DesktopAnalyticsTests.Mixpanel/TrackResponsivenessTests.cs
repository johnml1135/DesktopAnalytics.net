using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using DesktopAnalytics;
using NUnit.Framework;

namespace DesktopAnalyticsTests
{
	// EventSpool (DiskQueue) holds its global _sync semaphore across the network send, so a
	// UI-thread Track() call blocks for the WHOLE send -- up to ~45s against a stalled connection
	// in production. SqliteEventSpool claims a batch under a short lock, releases it, sends, then
	// resolves the claim under a second short lock, so Track() stays responsive throughout.
	// NB: the stall is EventSpool's lock, not a DiskQueue limitation -- DiskQueue supports
	// concurrent sessions (probed 2026-07-16), so dropping that lock would also fix it. See the
	// class remarks on SqliteEventSpool.
	[TestFixture]
	public class TrackResponsivenessTests
	{
		private string _spoolDir;

		[SetUp]
		public void SetUp()
		{
			_spoolDir = Path.Combine(Path.GetTempPath(), "TrackResponsivenessTests_" + Guid.NewGuid());
		}

		[TearDown]
		public void TearDown()
		{
			if (Directory.Exists(_spoolDir))
			{
				try
				{
					Directory.Delete(_spoolDir, true);
				}
				catch (Exception e)
				{
					Console.WriteLine("TrackResponsivenessTests.TearDown: failed to delete " + _spoolDir + ": " + e);
				}
			}
		}

		// Blocks inside SendBatchAsync until the test explicitly releases it (via a
		// TaskCompletionSource, never a real delay), so a test can hold a send genuinely "in
		// flight" for as long as it likes and then let it go on cue. Same shape as
		// MixpanelClientTests.BlockingUntilSignaledSender; duplicated here (rather than shared)
		// because that one is private to its fixture.
		private class BlockingUntilSignaledSender : IEventSender
		{
			private readonly TaskCompletionSource<bool> _release =
				new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
			public int CallCount;

			public void Release() => _release.TrySetResult(true);

			public async Task<BatchSendResult> SendBatchAsync(IReadOnlyList<AnalyticsEvent> events,
				CancellationToken cancellationToken = default)
			{
				Interlocked.Increment(ref CallCount);
				await _release.Task.ConfigureAwait(false);
				return new BatchSendResult(SendResult.Delivered);
			}
		}

		// Polls sender.CallCount (never a bare Thread.Sleep as the synchronization) until the
		// drain's send has genuinely started, so a test's timed Track() call races a send that is
		// truly in flight rather than one that hasn't begun yet.
		private static async Task WaitForSendToStart(BlockingUntilSignaledSender sender)
		{
			var deadline = DateTime.UtcNow.AddSeconds(5);
			while (sender.CallCount == 0 && DateTime.UtcNow < deadline)
				await Task.Delay(10);
			Assert.AreEqual(1, sender.CallCount, "the drain's send must have started before timing Track()");
		}

		// ---- THE REQUIREMENT: SqliteEventSpool keeps Track() responsive -------------------------

		[Test]
		public async Task Track_WhileSendInFlight_ReturnsImmediately_Sqlite()
		{
			using (var spool = new SqliteEventSpool(_spoolDir, 1000))
			{
				var sender = new BlockingUntilSignaledSender();
				var client = new MixpanelClient();
				client.InitializeForTest(spool, sender, batchSize: 5);

				client.Track("user-1", "Seed", null);

				var drainTask = client.DrainOnceAsync(); // Blocks inside the sender until Release().
				await WaitForSendToStart(sender);

				var stopwatch = Stopwatch.StartNew();
				client.Track("user-1", "WhileSendInFlight", null);
				stopwatch.Stop();

				// The send is blocked indefinitely until Release() below is called, so there is no
				// way for a correct (lock-not-held-across-send) implementation to take anywhere near
				// this long by accident. 500ms is generous headroom above the expected
				// single-digit-millisecond reality while still being unambiguous against the
				// multi-second-to-45s stalls this design exists to eliminate.
				Assert.Less(stopwatch.Elapsed, TimeSpan.FromMilliseconds(500),
					"Track() must return promptly even while a send is genuinely in flight -- " +
					"SqliteEventSpool never holds its lock across the network await");

				sender.Release();
				await drainTask;

				// Responsiveness must not come from silently dropping the event: it must have been
				// spooled (and, once the drain completes, delivered).
				Assert.AreEqual(1, client.Statistics.Succeeded,
					"the seed event must have been delivered by the drain this test released");
				await client.DrainOnceAsync();
				Assert.AreEqual(0, spool.ApproximateCount,
					"the Track() call made while the send was in flight must have been spooled, " +
					"not dropped, and must drain cleanly afterward");
				Assert.AreEqual(2, client.Statistics.Submitted);
			}
		}

		// ---- THE DEFECT (characterization, not a requirement): EventSpool blocks ---------------
		//
		// This test PASSES against today's EventSpool behavior on purpose -- it pins the documented
		// defect (see MixpanelClientTests.ConcurrentTrack_WhileDrainInProgress_..., whose comments
		// describe Track() threads "piling up" behind the lock) so it is visible in the test suite
		// rather than hidden in a comment. Delete it if EventSpool is dropped -- or if EventSpool's
		// global lock is narrowed so it stops blocking, in which case this test should start failing
		// and be deleted rather than "fixed".
		[Test]
		public async Task Track_WhileSendInFlight_BlocksUntilSendCompletes_DiskQueue()
		{
			using (var spool = new EventSpool(_spoolDir, 1000))
			{
				var sender = new BlockingUntilSignaledSender();
				var client = new MixpanelClient();
				client.InitializeForTest(spool, sender, batchSize: 5);

				client.Track("user-1", "Seed", null);

				var drainTask = client.DrainOnceAsync(); // Blocks inside the sender until Release().
				await WaitForSendToStart(sender);

				// Run Track() on its own thread so this test can observe it NOT finishing while the
				// send is blocked, rather than deadlocking the test itself.
				var trackFinished = new ManualResetEventSlim(false);
				var trackThread = new Thread(() =>
				{
					client.Track("user-1", "WhileSendInFlight", null);
					trackFinished.Set();
				});
				trackThread.Start();

				// EventSpool's transactional session holds its lock across the whole send, so
				// Track() must still be blocked after a short window -- this is the defect, pinned.
				var finishedWhileBlocked = trackFinished.Wait(TimeSpan.FromMilliseconds(300));
				Assert.IsFalse(finishedWhileBlocked,
					"characterizes today's known defect: EventSpool holds its lock across the " +
					"network send, so Track() blocks for the whole send instead of returning " +
					"promptly. If this ever fails, EventSpool has stopped blocking Track() and this " +
					"test (and the bug it documents) should be deleted, not fixed.");

				// Release the blocked send so the Track() thread (and the drain) can finally
				// complete -- otherwise this test would hang forever.
				sender.Release();

				Assert.IsTrue(trackFinished.Wait(TimeSpan.FromSeconds(5)),
					"Track() must complete once the send is released");
				trackThread.Join();
				await drainTask;

				Assert.AreEqual(1, client.Statistics.Succeeded);
				await client.DrainOnceAsync();
				Assert.AreEqual(0, spool.ApproximateCount);
				Assert.AreEqual(2, client.Statistics.Submitted);
			}
		}
	}
}
