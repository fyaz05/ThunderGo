package bot

// Message templates for all bot responses.
// msgReady is the standard — other link/file messages must match its vocabulary.

// Community invite link.
const communityLink = "https://t.me/+_gIfFDRAJ8liYzVl"
const communityName = "Thunder Community"

// theme holds emojis for inline buttons and markers.
var theme = struct {
	Stream    string
	Download  string
	Cancel    string
	Recycle   string
	Help      string
	About     string
	Close     string
	GitHub    string
	Start     string
	Join      string
	Community string
}{
	Stream:    "🖥️",
	Download:  "🚀",
	Cancel:    "🛑",
	Recycle:   "♻️",
	Help:      "📖",
	About:     "ℹ️",
	Close:     "✖️",
	GitHub:    "🛠️",
	Start:     "📩",
	Join:      "📢",
	Community: "💬",
}

const msgWelcome = `<b>⚡ Welcome to ThunderGo, %s!</b>

I turn any Telegram file into a direct <b>streaming</b> + <b>download</b> link — instantly. 🚀

<blockquote>📦 Send me any file in private chat
🔗 In groups, reply to a media message with <code>/link</code>
📚 Use <code>/link 5</code> for batch processing (up to %d files)
🎬 Stream links support seeking in any browser</blockquote>

<i>Tap the buttons below to explore, or send a file to begin!</i>`

const msgNewUser = `<b>✨ New User Alert!</b> ✨

<blockquote>👤 <b>Name:</b> <a href="tg://user?id=%d">%s</a>
🆔 <b>User ID:</b> <code>%d</code></blockquote>`

const msgProcessing = "⏳ <b>Processing your file...</b>"
const msgProcessingN = "⏳ <b>Processing %d files...</b>"

// Batch live-progress messages.
const msgProcessingBatch = "♻️ <b>Processing Batch %d/%d</b> (%d files)"
const msgProcessingStatus = "📊 <b>Processing Files:</b> %d/%d complete, %d failed"

// Link message — standard for all file/link messages.

const msgReady = `✨ <b>Your Links are Ready!</b> ✨%s

<blockquote><code>%s</code></blockquote>

📂 <b>File Size:</b> <code>%s</code>
📎 <b>Type:</b> <code>%s</code>

🚀 <b>Download Link:</b>
<code>%s</code>

🖥️ <b>Stream Link:</b>
<code>%s</code>

<blockquote>⌛️ <b>Note: %s</b></blockquote>`

// Batch link message (compact, no size/type).
const msgBatchReady = `✨ <b>Links Ready</b> ✨%s

<blockquote><code>%s</code></blockquote>

🚀 <b>Download Link:</b> <code>%s</code>
🖥️ <b>Stream Link:</b> <code>%s</code>`

// DM prefix for links sent privately from a group.
const msgDMSinglePrefix = "📬 <b>From %s</b>\n"
const msgDMBatchPrefix = "📬 <b>Batch Links from %s</b>\n"

// Batch links header.
const msgBatchLinksReady = "🔗 <b>Here are your %d download links:</b>"

const msgErrInternal = `<b>⚠️ Oops!</b> Something went wrong.

<blockquote>Please try again. If the issue persists, contact support.</blockquote>`

const msgErrProcessFile = `<b>⚠️ Oops!</b> Something went wrong while processing your media.

<blockquote>Please try again. If the issue persists, contact support.</blockquote>`

const msgErrFetchMsg = "⚠️ <b>Please use the /link command in reply to a file.</b>"
const msgErrNoMedia = "⚠️ <b>The replied-to message does not contain any file.</b>"
const msgErrPostStatus = "❌ <b>Could not post a status message.</b> Check my admin permissions in this chat."

const msgErrNotAdmin = `<b>⚠️ Admin Required</b>

<blockquote>I need admin privileges to work here.</blockquote>`

const msgErrDMBlocked = `<b>⚠️ DM Failed</b>

<blockquote>I couldn't send you a Direct Message. Please start the bot first.</blockquote>`

const msgUsageLinkGroup = `<b>ℹ️ /link is for Groups</b>

<blockquote>💬 In <b>private chat</b>, just send me a file directly — no command needed.
👥 Use <code>/link</code> only when replying to a media message in a group.</blockquote>`

const msgUsageLinkReply = `<b>ℹ️ How to Use /link</b>

<blockquote>👆 Reply to a media message (video, document, photo, audio) with <code>/link</code>.
📚 For batch mode: <code>/link 5</code> processes the next 5 messages.</blockquote>`

const msgUsageLinkN = "⚠️ <b>Invalid number specified.</b>"
const msgUsageLinkNRange = "⚠️ <b>Please specify a number between 1 and %d.</b>"

const msgUsageLinkPrivate = `<b>⚠️ You need to start the bot in private first.</b>

<blockquote>👉 Tap the button below to start a private chat, then send <code>/start</code>.</blockquote>`

const msgBannedNotice = `<b>🚫 You have been banned from using this bot.</b>`

const msgBannedNoticeReason = `<b>🚫 You have been banned from using this bot.</b>

<blockquote>📝 <b>Reason:</b> %s</blockquote>`

const msgBannedUser = `<b>✅ User %d has been banned.</b>%s`

const msgBannedChannel = `<b>✅ Channel %d has been banned.</b>%s`

const msgBannedSelf = "❌ <b>You cannot ban yourself.</b>"
const msgBannedInvalidChannel = "❌ <b>Invalid channel ID.</b>\nMust start with <code>-100</code>."

const msgUnbannedUser = `<b>✅ User %d has been unbanned.</b>`

const msgUnbannedChannel = `<b>✅ Channel %d has been unbanned.</b>`

const msgUnbannedNotice = `<b>🎉 You have been unbanned from using this bot.</b>`

const msgBannedReasonSuffix = "\n📝 <b>Reason:</b> %s"

const msgBroadcastStart = `<b>📣 Starting Broadcast...</b>

<blockquote>⏳ Please wait for completion.</blockquote>`

const msgBroadcastComplete = `<b>📢 Broadcast Completed Successfully!</b> 📢

<blockquote>⏱️ <b>Duration:</b> <code>%s</code>
📊 <b>Mode:</b> %s
👥 <b>Total Users:</b> <code>%d</code>
✅ <b>Successful Deliveries:</b> <code>%d</code>
❌ <b>Failed Deliveries:</b> <code>%d</code>
🗑️ <b>Accounts Removed (Blocked/Deactivated):</b> <code>%d</code></blockquote>`

const msgBroadcastCancelled = `<b>🛑 Broadcast Cancelled</b>

<blockquote>⏱️ <b>Duration:</b> <code>%s</code>
📊 <b>Mode:</b> %s
👥 <b>Total Users:</b> <code>%d</code>
✅ <b>Sent before cancel:</b> <code>%d</code>
❌ <b>Failed:</b> <code>%d</code>
🗑️ <b>Unreachable (pruned):</b> <code>%d</code></blockquote>`

// Intermediate cancelling message.
const msgBroadcastCancelling = `<b>🛑 Cancelling Broadcast:</b> <code>%s</code>

<blockquote>⏳ Stopping operations...</blockquote>`

const msgBroadcastUsage = `<b>📣 Broadcast Command Usage</b>

<blockquote><code>/broadcast</code> - Broadcast to all users
<code>/broadcast authorized</code> - Broadcast to authorized users only
<code>/broadcast regular</code> - Broadcast to regular (non-authorized) users only

<b>Note:</b> Reply to the message you want to broadcast.</blockquote>`

const msgBroadcastModeAll = "all users"
const msgBroadcastModeAuthorized = "authorized users"
const msgBroadcastModeRegular = "regular users"

const msgAuthorized = `<b>✅ User Authorized!</b>

<blockquote>🆔 <b>User ID:</b> <code>%d</code>
👤 <b>Name:</b> <code>%s</code>
🔑 <b>Access:</b> Permanent</blockquote>`

const msgDeauthorized = `<b>✅ User Deauthorized!</b>

<blockquote>🆔 <b>User ID:</b> <code>%d</code>
🔒 <b>Access:</b> Revoked</blockquote>`

const msgAuthUserNotFound = "❗ <b>User Not Found:</b> Couldn't find user. Please check the ID or Username."

const msgAuthListHeader = `<b>🔐 Authorized Users List</b>`

const msgAuthOwnerImplicit = "\n👑 <b>Owner:</b> <code>%d</code> <i>(implicit, all access)</i>"

const msgBotStatus = `<b>✅ System Status: Operational</b>

<blockquote>🕒 <b>Uptime:</b> <code>%s</code>
🤖 <b>Bot:</b> @%s
🏷️ <b>Version:</b> <code>%s</code>
🔗 <b>Bot Instances:</b> <code>%d</code>
⚡ <b>Total Workload:</b> <code>%d</code></blockquote>

<b>📜 Workload Distribution:</b>

%s`

const msgUserCount = `<b>📊 Database Statistics</b>

<blockquote>👥 <b>Total Users:</b> <code>%d</code></blockquote>`

const msgLogEmpty = "ℹ️ <b>Log File Empty:</b> No data found in the log file."
const msgLogMissing = "⚠️ <b>Log File Missing:</b> Could not find the log file."
const msgLogCaption = "📄 <b>System Logs</b> (<code>%d</code>)"
const msgLogCaptionTailed = "📄 <b>System Logs</b> (last %d MiB of %d bytes)"
const msgLogReadErr = "❌ <b>Could not read log file.</b> Please try again later."
const msgLogSendErr = "❌ <b>Could not send log file.</b> Please try again later."
const msgLogPrepErr = "❌ <b>Could not prepare log file.</b> Please try again later."

const msgRestarting = `<b>♻️ Updating and Restarting Bot...</b>

<blockquote>⏳ Please wait a moment.</blockquote>`

const msgRestartFailed = `<b>❌ Restart Failed</b>

<blockquote><code>%s</code></blockquote>`

const msgRestartSuccess = `<b>✅ Restart Successful!</b>`

const msgPinging = "🛰️ <b>Pinging...</b> Please wait."

const msgPong = `<b>☁️ PONG! Bot is Online!</b> ⚡

<blockquote>⏱️ <b>Ping:</b> <code>%.2f ms</code>
🤖 <b>Bot Status:</b> <code>Active</code></blockquote>`

const msgFileDC = `<b>🗂️ File Information</b>

<blockquote><code>%s</code></blockquote>

📂 <b>File Size:</b> <code>%s</code>
📎 <b>Type:</b> <code>%s</code>
🌍 <b>DC ID:</b> <code>%d</code>`

const msgUserDC = `<b>📍 Information</b>

<blockquote>👤 <b>User:</b> <a href="tg://user?id=%d">%s</a>
🆔 <b>User ID:</b> <code>%d</code>
🌍 <b>DC ID:</b> <code>%d</code></blockquote>`

const msgYourDC = `<b>📍 Information</b>

<blockquote>👤 <b>User:</b> <a href="tg://user?id=%d">%s</a>
🆔 <b>User ID:</b> <code>%d</code>
🌍 <b>DC ID:</b> <code>%d</code></blockquote>`

// DC error messages.
const msgDCInvalidUsage = "🤔 <b>Invalid Usage:</b> Please reply to a user's message or a media file to get DC info."
const msgDCAnonError = "😥 <b>Cannot Get Your DC Info:</b> Unable to identify you. This command might not work for anonymous users."
const msgDCFileError = "⚙️ <b>Error Getting File DC Info:</b> Could not fetch details. File might be inaccessible."
const msgDCUnknown = "Unknown"

const msgBatchSummary = `<b>✅ Process Complete:</b> %d/%d files processed successfully, %d failed`

const msgBatchDMFailed = `<b>⚠️ Partial DM Delivery</b>

<blockquote>📭 I couldn't deliver %d batch chunk(s) to you in private chat.
This usually means you haven't started a private chat with me, or you've blocked me.</blockquote>`

const msgPrivateMode = `<b>🔒 Private Bot</b>

<blockquote>This bot is private. You are not authorized to use it.
Contact the bot owner if you believe you should have access.</blockquote>`

const msgRateLimited = `<b>⏳ Rate Limit Reached!</b>

<blockquote>⌛ <b>Estimated Wait:</b> <code>~%d second(s)</code>
📊 <b>Limit:</b> <code>%d files per minute</code>
🔄 <b>Status:</b> In Queue</blockquote>`

// Global rate-limit message (system-wide RPS cap).
const msgGlobalRateLimited = `<b>⚠️ Service Busy!</b> The processing queue is currently full.

<blockquote>🕒 <b>Please try again in:</b> <code>~%d second(s)</code>
💡 <b>Tip:</b> Try again later when system load decreases</blockquote>`

const msgForceSub = `<b>📢 %s</b>

<blockquote>🔒 Join this channel to use the bot.</blockquote>`

const msgForceSubButton = "📢 Join Channel"
const msgTempDBError = `<b>⚠️ Temporary Database Error</b>

<blockquote>A temporary database error occurred. Please try again in a moment.</blockquote>`

const msgActivationRequired = `<b>🔒 Activation Required</b>

<blockquote>🔑 You need to activate your account before using this bot.
Tap the button below to get your access token.</blockquote>`

const msgActivationButton = "🔑 Get Activation Token"

const msgActivated = `<b>✅ Token successfully activated!</b>

<blockquote>⏳ This token is valid for <b>%s</b>.</blockquote>`

const msgActivationInvalid = `<b>🚫 Expired or Invalid Token.</b>

<blockquote>Please click the button below to activate your access token.</blockquote>`

const msgCallbackHelpAnswered = "📖 Help sent"
const msgCallbackAboutAnswered = "ℹ️ About sent"
const msgCallbackCloseAnswered = "✅ Closed"
const msgCallbackCloseDenied = "⚠️ Only the message owner can close this."
const msgCallbackUnsupported = "⚠️ This button is not active or no longer supported."

// Admin command usage hints.
const msgUsageBan = "Usage: <code>/ban &lt;user_id&gt; [reason]</code>"
const msgUsageUnban = "Usage: <code>/unban &lt;user_id&gt;</code>"
const msgUsageAuthorize = "Usage: <code>/authorize &lt;user_id&gt;</code>"
const msgUsageDeauthorize = "Usage: <code>/deauthorize &lt;user_id&gt;</code>"

// Validation errors.
const msgErrInvalidID = "❌ Invalid ID."
const msgErrInvalidIDInt = "❌ Invalid ID. Must be an integer."
const msgErrBotNotReady = "❌ Bot client is not ready. Please try again later."

// Fallback display names.
const msgFallbackUserName = "User"
const msgFallbackChatTitle = "the chat"
const msgFallbackUnknownName = "(unknown)"

// File expiry notes (used by fileExpiryNote).
const msgFileExpiryNever = "Links remain active while the bot is running and the file is accessible."
const msgFileExpiryDays = "Links expire in %d day(s)."

// File type labels (used by friendlyFileType).
const msgFileTypeVideo = "🎬 Video"
const msgFileTypePhoto = "🖼️ Photo"
const msgFileTypeAudio = "🎵 Audio"
const msgFileTypeVoice = "🎤 Voice Message"
const msgFileTypeSticker = "🎨 Sticker"
const msgFileTypeAnimation = "🎞️ Animation (GIF)"
const msgFileTypeDocument = "📄 Document"
const msgFileTypeUnknown = "❓ Unknown File Type"

// Callback alert messages (broadcast).
const msgCallbackBroadcastCancelDenied = "Only the owner can cancel a broadcast."
const msgCallbackBroadcastNotFound = "No active broadcast found for this message."
const msgCallbackBroadcastCancelled = "Broadcast cancelled."

// DB operation error (wraps the raw error).
const msgErrDBOperation = "❌ %s"
