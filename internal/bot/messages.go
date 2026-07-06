package bot

// theme holds the emojis used in inline buttons. A var (not const) because Go
// does not permit struct-typed constants.
var theme = struct {
	Stream   string
	Download string
	Cancel   string
	Recycle  string
}{
	Stream:   "▶️",
	Download: "⬇️",
	Cancel:   "🛑",
	Recycle:  "♻️",
}

// User-facing message constants. Interpolate via fmt.Sprintf; user-supplied
// values MUST be passed through html.EscapeString first.

// --- Welcome / Start ---
const msgWelcome = `👋 <b>Welcome, %s!</b>

Send me a file in private chat and I'll give you a streaming + download link.
In groups, reply to a media message with <code>/link</code>.

Use <code>/help</code> for full usage.`

const msgNewUser = `👤 <b>New user</b>

<b>Name:</b> %s
<b>Username:</b> %s
<b>ID:</b> %d`

// --- Processing ---
const msgProcessing = "⏳ Processing…"
const msgProcessingN = "⏳ Processing %d files…"

// --- Ready (file links) ---
const msgReady = `✅ <b>Ready%s</b>

<b>File:</b> %s
<b>Size:</b> %d bytes
<b>Type:</b> %s

▶️ <b>Stream:</b> %s
⬇️ <b>Download:</b> %s`

const msgBatchReady = `✅ <b>Ready%s</b>

<b>File:</b> %s
▶️ <b>Stream:</b> %s
⬇️ <b>Download:</b> %s`

// --- Errors ---
const msgErrInternal = "❌ An error occurred. Please try again later."
const msgErrProcessFile = "❌ Could not process this file. Please try again later."
const msgErrFetchMsg = "❌ Could not fetch the replied-to message."
const msgErrNoMedia = "❌ The replied-to message has no media."
const msgErrPostStatus = "❌ Could not post status message. Try again."
const msgErrNotAdmin = "⚠️ I need admin rights in this group to work here."
const msgErrDMBlocked = "⚠️ I couldn't send you a private message. Please make sure you haven't blocked me."

// --- Usage hints ---
const msgUsageLinkGroup = "ℹ️ <b>/link</b> works in groups. In private chat, just send me a file directly."
const msgUsageLinkReply = "ℹ️ Reply to a media message with <code>/link</code> to generate a link."
const msgUsageLinkN = "❌ Invalid argument. Usage: <code>/link [N]</code>"
const msgUsageLinkNRange = "❌ N must be between 1 and %d."
const msgUsageLinkPrivate = "ℹ️ Please start me in private chat first, then try again."

// --- Ban / Unban ---
const msgBannedNotice = "🚫 You have been banned from using this bot."
const msgBannedNoticeReason = "🚫 You have been banned from using this bot.\nReason: %s"
const msgBannedUser = "✅ Banned user <code>%d</code>."
const msgBannedChannel = "✅ Banned channel <code>%d</code>."
const msgBannedSelf = "❌ You cannot ban yourself."
const msgBannedInvalidChannel = "❌ Invalid channel ID; must start with -100."
const msgUnbannedUser = "✅ Unbanned user <code>%d</code>."
const msgUnbannedChannel = "✅ Unbanned channel <code>%d</code>."
const msgUnbannedNotice = "✅ You have been unbanned."

// --- Broadcast ---
const msgBroadcastStart = "📢 Broadcasting…"
const msgBroadcastComplete = `✅ <b>Broadcast complete</b>

Mode: %s
Sent: %d
Failed: %d
Unreachable: %d (pruned from DB)
Total users: %d`

const msgBroadcastCancelled = `🛑 <b>Broadcast cancelled</b>

Mode: %s
Sent: %d
Failed: %d
Unreachable: %d (pruned from DB)
Total users: %d`

const msgBroadcastUsage = "ℹ️ Reply to a message with <code>/broadcast [all|authorized|regular]</code> to send it."

// Broadcast mode labels shown in the summary line.
const msgBroadcastModeAll = "all users"
const msgBroadcastModeAuthorized = "authorized users"
const msgBroadcastModeRegular = "regular users"

// --- Authorize ---
const msgAuthorized = "✅ Authorized <code>%d</code> (%s)."
const msgDeauthorized = "✅ Deauthorized <code>%d</code>."
const msgAuthUserNotFound = "❌ Could not find that user. Please check the ID."
const msgAuthListHeader = "📋 <b>Authorized users (%d)</b> — page %d/%d"
const msgAuthOwnerImplicit = "\n👑 Owner: <code>%d</code> (implicit)"

// --- Status ---
const msgBotStatus = `📊 <b>Bot status</b>

<b>Version:</b> %s
<b>Bot:</b> @%s
<b>Uptime:</b> %s
<b>Active clients:</b> %d
<b>Total in-flight:</b> %d

<b>Per-client workload:</b>
%s`

// --- Users ---
const msgUserCount = "👥 <b>Total users:</b> %d"

// --- Log ---
const msgLogEmpty = "ℹ️ Log file is empty or missing."
const msgLogCaption = "📄 Log file (%d bytes)"
const msgLogCaptionTailed = "📄 Log file (last %d MiB of %d bytes)"
const msgLogReadErr = "❌ Could not read log file. Please try again later."
const msgLogSendErr = "❌ Could not send log file. Please try again later."
const msgLogPrepErr = "❌ Could not prepare log file. Please try again later."

// --- Restart ---
const msgRestarting = "🔄 Restarting… (pulling latest + rebuilding)"
const msgRestartFailed = "❌ Restart failed: <code>%s</code>"
const msgRestartSuccess = "✅ Restart Successful"

// --- Ping ---
const msgPinging = "Pinging…"
const msgPong = "🏓 Pong! <code>%dms</code>"

// --- DC ---
const msgFileDC = `📁 <b>File DC</b>

<b>DC:</b> %d
<b>Name:</b> %s
<b>Size:</b> %d bytes
<b>Type:</b> %s`

const msgUserDC = `👤 <b>User DC</b>

<b>DC:</b> %d
<b>Name:</b> %s`

const msgYourDC = "👤 <b>Your DC</b>\n\n<b>DC:</b> %d"

// --- Batch summary ---
const msgBatchSummary = "✅ Processed %d, ⏭️ skipped %d, ❌ failed %d"
const msgBatchDMFailed = "⚠️ Could not deliver %d batch DM chunk(s) to you in private chat (you may have blocked me)."

// --- Pre-flight rejections ---
const msgPrivateMode = "🔒 This bot is private. You are not authorized to use it."
const msgRateLimited = "⏳ Rate limit reached. Please try again in %ds."
const msgForceSub = "🔔 Please join our channel to use this bot."
const msgForceSubButton = "🔔 Join Channel"
const msgTempDBError = "⚠️ Temporary database error. Please try again."

// --- Token Activation ---
const msgActivationRequired = `🔒 <b>Activation Required</b>

You need to activate to use this bot. Click the button below to get your access token.`
const msgActivationButton = "🔑 Get Activation Token"
const msgActivated = `✅ <b>Activation Successful!</b>

You can now use the bot for %s.`
const msgActivationInvalid = "❌ Invalid or expired activation token. Please try again."

// --- Inline-button callback answers ---
// Short pop-up/toast texts sent via cq.Answer() when a user taps an inline button.
const msgCallbackHelpAnswered = "📖 Help"
const msgCallbackAboutAnswered = "🤖 About"
const msgCallbackCloseAnswered = "✅ Closed"
