using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DesktopAnalytics
{
	/// <summary>
	/// The durable, bounded store of pending <see cref="AnalyticsEvent"/>s behind
	/// <see cref="MixpanelClient"/>. Implemented by <see cref="EventSpool"/> (DiskQueue) and
	/// <see cref="SqliteEventSpool"/> (SQLite); see offline-analytics.md for the comparison.
	/// </summary>
	/// <remarks>
	/// Contract shared by every implementation:
	/// <list type="bullet">
	/// <item><b>Never throws.</b> Disk/IO/serialization failures are logged and swallowed --
	/// analytics must never crash the host. (Constructors may throw; that is a startup-time
	/// condition the caller needs to know about.)</item>
	/// <item><b>At-least-once delivery.</b> An event is removed only after
	/// <see cref="ProcessBatchAsync"/>'s callback confirms the batch's fate. A crash between a
	/// successful send and that removal replays the batch, which Mixpanel deduplicates via
	/// <see cref="AnalyticsEvent.InsertId"/>.</item>
	/// <item><b>FIFO.</b> Events are gathered oldest-first, and eviction drops oldest-first.</item>
	/// </list>
	/// </remarks>
	internal interface IEventSpool : IDisposable
	{
		/// <summary>
		/// Raised once per event permanently dropped by cap enforcement -- i.e. NOT a drop at
		/// enqueue time (which <see cref="Enqueue"/>'s return value already reports), but an older
		/// event evicted later to stay within the configured caps. This is the only way callers
		/// learn of that kind of drop, since it can happen on an <see cref="Enqueue"/> call for a
		/// different event than the one dropped. Handlers must be fast and must not call back into
		/// the spool.
		/// </summary>
		event Action ItemDroppedByCap;

		/// <summary>An approximate count of events currently spooled. Accurate enough for
		/// bounding and diagnostics; not a hard guarantee under concurrent access.</summary>
		int ApproximateCount { get; }

		/// <summary>The spool's current logical byte total -- the value the byte cap is enforced
		/// against.</summary>
		long ApproximateBytes { get; }

		/// <summary>
		/// Serializes and enqueues <paramref name="evt"/>, then enforces the configured caps by
		/// dropping the oldest event(s) if necessary.
		/// </summary>
		/// <returns>True if durably enqueued; false if dropped at enqueue time (null,
		/// unserializable, over the per-event byte cap, or a disk failure).</returns>
		bool Enqueue(AnalyticsEvent evt);

		/// <summary>
		/// Gathers up to <paramref name="maxItems"/> events (oldest first, bounded by
		/// <paramref name="maxBytes"/>), hands them to <paramref name="send"/> as ONE batch, and
		/// commits or rolls back on its verdict: <see cref="SendResult.Delivered"/> and
		/// <see cref="SendResult.PoisonDrop"/> remove the batch; <see cref="SendResult.RetryableFailure"/>
		/// (or a throwing <paramref name="send"/>) leaves it spooled, in order, for a later retry.
		/// An entry that cannot be deserialized is dropped rather than allowed to wedge the spool.
		/// </summary>
		/// <param name="maxBytes">Soft byte budget for the batch, checked AFTER each gathered event
		/// so a single oversized event still goes -- the budget can never starve the queue.</param>
		Task ProcessBatchAsync(int maxItems,
			Func<IReadOnlyList<AnalyticsEvent>, CancellationToken, Task<SendResult>> send,
			long maxBytes = long.MaxValue, CancellationToken cancellationToken = default);

		/// <summary>
		/// Age-based retention floor: drops events whose stamped <see cref="AnalyticsEvent.Time"/>
		/// is older than <paramref name="now"/> minus <paramref name="maxAge"/>. An independent
		/// eviction dimension alongside the item/byte caps, not a replacement for them.
		/// </summary>
		/// <param name="now">From the caller's injected <see cref="TimeProvider"/> so tests stay
		/// deterministic -- never <c>DateTimeOffset.UtcNow</c> directly.</param>
		void TrimExpired(TimeSpan maxAge, DateTimeOffset now);

		/// <summary>
		/// Empties the spool entirely (consent revocation), removing the event data from disk --
		/// not merely marking it consumed.
		/// </summary>
		void Purge();
	}
}
