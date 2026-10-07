const http = require("http");
const WebSocket = require("ws");
const crypto = require("crypto");

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
    res.writeHead(200, {
        "Content-Type": "application/json"
    });

    res.end(
        JSON.stringify({
            status: "online",
            service: "ChitChat",
            version: "2.0"
        })
    );
});

const wss = new WebSocket.Server({
    server: server
});

const waitingUsers = [];
const users = new Map();

// Temporary in-memory blocks/reports.
// These will reset when Render restarts.
const blockedPairs = new Set();
const reports = [];


// ============================================================
// HELPERS
// ============================================================

function generateId() {
    return crypto.randomUUID();
}


function getClientIp(req) {
    const forwarded = req.headers["x-forwarded-for"];

    if (forwarded) {
        return forwarded.split(",")[0].trim();
    }

    return req.socket.remoteAddress || "unknown";
}


function send(ws, data) {
    if (
        ws &&
        ws.readyState === WebSocket.OPEN
    ) {
        ws.send(JSON.stringify(data));
    }
}


function removeFromQueue(ws) {
    const index = waitingUsers.indexOf(ws);

    if (index !== -1) {
        waitingUsers.splice(index, 1);
    }
}


function getUser(ws) {
    return users.get(ws);
}


function isBlocked(userId1, userId2) {
    return (
        blockedPairs.has(`${userId1}:${userId2}`) ||
        blockedPairs.has(`${userId2}:${userId1}`)
    );
}


function blockPair(userId1, userId2) {
    blockedPairs.add(`${userId1}:${userId2}`);
    blockedPairs.add(`${userId2}:${userId1}`);
}


// ============================================================
// ACTIVE USERS
// ============================================================

function broadcastActiveUsers() {
    const count = users.size;

    for (const client of users.keys()) {
        send(client, {
            type: "active_users",
            count: count
        });
    }
}


// ============================================================
// PARTNER DISCONNECT
// ============================================================

function disconnectFromPartner(ws, reason = "partner_left") {

    const user = users.get(ws);

    if (!user || !user.partner) {
        return;
    }

    const partner = user.partner;

    user.partner = null;
    user.typing = false;

    if (users.has(partner)) {

        const partnerUser = users.get(partner);

        partnerUser.partner = null;
        partnerUser.typing = false;

        send(partner, {
            type: "partner_left",
            reason: reason
        });
    }
}


// ============================================================
// FIND STRANGER
// ============================================================

function findStranger(ws) {

    const user = users.get(ws);

    if (!user) {
        return;
    }

    removeFromQueue(ws);

    if (user.partner) {
        return;
    }

    while (waitingUsers.length > 0) {

        const stranger = waitingUsers.shift();

        if (
            !stranger ||
            stranger.readyState !== WebSocket.OPEN ||
            !users.has(stranger)
        ) {
            continue;
        }

        if (stranger === ws) {
            continue;
        }

        const strangerUser = users.get(stranger);

        if (!strangerUser) {
            continue;
        }

        if (strangerUser.partner) {
            continue;
        }

        // Don't match users who blocked each other.
        if (
            isBlocked(
                user.id,
                strangerUser.id
            )
        ) {
            continue;
        }

        // IMPORTANT:
        // We no longer block same-IP users.
        //
        // This allows:
        // PC + mobile on same Wi-Fi
        // to test ChitChat properly.
        //
        // IP should not be used as a unique user ID.

        user.partner = stranger;
        strangerUser.partner = ws;

        user.searching = false;
        strangerUser.searching = false;

        user.typing = false;
        strangerUser.typing = false;

        send(ws, {
            type: "matched",
            partnerId: strangerUser.id
        });

        send(stranger, {
            type: "matched",
            partnerId: user.id
        });

        return;
    }

    user.searching = true;

    waitingUsers.push(ws);

    send(ws, {
        type: "waiting"
    });
}


// ============================================================
// MESSAGE
// ============================================================

function sendChatMessage(ws, data) {

    const user = users.get(ws);

    if (!user || !user.partner) {
        return;
    }

    const partner = user.partner;

    if (
        !partner ||
        !users.has(partner)
    ) {
        return;
    }

    const text = String(
        data.text || ""
    ).trim();

    if (!text) {
        return;
    }

    // Basic message length protection.
    if (text.length > 2000) {
        send(ws, {
            type: "error",
            message: "Message is too long."
        });

        return;
    }

    const messageId = generateId();
    const clientMessageId =
        String(data.clientMessageId || "");

    const timestamp = Date.now();
    const replyTo = data.replyTo || null;

    const message = {
        type: "message",
        messageId: messageId,
        clientMessageId: clientMessageId,
        text: text,
        timestamp: timestamp,
        replyTo: replyTo
    };

    // Send to partner.
    send(partner, message);

    // Tell sender that the server accepted it.
    send(ws, {
        type: "message_sent",
        messageId: messageId,
        clientMessageId: clientMessageId,
        timestamp: timestamp
    });
}


// ============================================================
// MESSAGE DELIVERED
// ============================================================

function messageDelivered(ws, data) {

    const user = users.get(ws);

    if (!user || !user.partner) {
        return;
    }

    const messageId =
        String(data.messageId || "");

    if (!messageId) {
        return;
    }

    // Tell the sender that partner received the message.
    send(user.partner, {
        type: "message_delivered",
        messageId: messageId
    });
}


// ============================================================
// MESSAGE REACTION
// ============================================================

function reactToMessage(ws, data) {

    const user = users.get(ws);

    if (!user || !user.partner) {
        return;
    }

    const messageId =
        String(data.messageId || "");

    const emoji =
        String(data.emoji || "");

    if (!messageId || !emoji) {
        return;
    }

    // Limit reaction length.
    if (emoji.length > 10) {
        return;
    }

    send(user.partner, {
        type: "reaction",
        messageId: messageId,
        emoji: emoji
    });
}


// ============================================================
// REPORT
// ============================================================

function reportUser(ws, data) {

    const user = users.get(ws);

    if (!user || !user.partner) {
        return;
    }

    const partnerUser =
        users.get(user.partner);

    if (!partnerUser) {
        return;
    }

    const reason =
        String(data.reason || "Other")
            .substring(0, 100);

    const report = {
        id: generateId(),

        reporterId: user.id,

        reportedId: partnerUser.id,

        reason: reason,

        timestamp: Date.now(),

        reporterIp: user.ip
    };

    reports.push(report);

    console.log(
        "REPORT:",
        JSON.stringify(report)
    );

    send(ws, {
        type: "report_submitted"
    });

    // Disconnect the reported user.
    disconnectFromPartner(
        ws,
        "reported"
    );

    user.searching = false;

    removeFromQueue(ws);

    send(ws, {
        type: "stopped"
    });
}


// ============================================================
// BLOCK
// ============================================================

function blockUser(ws) {

    const user = users.get(ws);

    if (!user || !user.partner) {
        return;
    }

    const partner =
        users.get(user.partner);

    if (!partner) {
        return;
    }

    blockPair(
        user.id,
        partner.id
    );

    console.log(
        `BLOCK: ${user.id} blocked ${partner.id}`
    );

    send(ws, {
        type: "blocked"
    });

    disconnectFromPartner(
        ws,
        "blocked"
    );

    user.searching = true;

    findStranger(ws);
}


// ============================================================
// TYPING
// ============================================================

function handleTyping(ws, data) {

    const user = users.get(ws);

    if (!user || !user.partner) {
        return;
    }

    const typing =
        data.typing === true;

    user.typing = typing;

    send(user.partner, {
        type: "typing",
        typing: typing
    });
}


// ============================================================
// STOP
// ============================================================

function stopUser(ws) {

    const user = users.get(ws);

    if (!user) {
        return;
    }

    removeFromQueue(ws);

    user.searching = false;
    user.typing = false;

    disconnectFromPartner(
        ws,
        "stopped"
    );

    send(ws, {
        type: "stopped"
    });
}


// ============================================================
// CONNECTION
// ============================================================

wss.on("connection", (ws, req) => {

    const ip = getClientIp(req);

    const userId = generateId();

    console.log(
        "Connected:",
        userId,
        ip
    );

    users.set(ws, {

        id: userId,

        ip: ip,

        partner: null,

        searching: false,

        typing: false
    });

    send(ws, {
        type: "connected",

        userId: userId
    });

    broadcastActiveUsers();


    // ========================================================
    // MESSAGE
    // ========================================================

    ws.on("message", (raw) => {

        let data;

        try {

            data = JSON.parse(
                raw.toString()
            );

        } catch (error) {

            send(ws, {
                type: "error",
                message: "Invalid request."
            });

            return;
        }

        const user = users.get(ws);

        if (!user) {
            return;
        }

        switch (data.type) {

            // ------------------------------------------------
            // START
            // ------------------------------------------------

            case "start":

                if (user.partner) {
                    return;
                }

                user.searching = true;

                findStranger(ws);

                break;


            // ------------------------------------------------
            // MESSAGE
            // ------------------------------------------------

            case "message":

                sendChatMessage(
                    ws,
                    data
                );

                break;


            // ------------------------------------------------
            // DELIVERED
            // ------------------------------------------------

            case "delivered":

                messageDelivered(
                    ws,
                    data
                );

                break;


            // ------------------------------------------------
            // REACTION
            // ------------------------------------------------

            case "reaction":

                reactToMessage(
                    ws,
                    data
                );

                break;


            // ------------------------------------------------
            // TYPING
            // ------------------------------------------------

            case "typing":

                handleTyping(
                    ws,
                    data
                );

                break;


            // ------------------------------------------------
            // SKIP
            // ------------------------------------------------

            case "skip":

                disconnectFromPartner(
                    ws,
                    "skipped"
                );

                user.searching = true;

                user.typing = false;

                findStranger(ws);

                break;


            // ------------------------------------------------
            // STOP
            // ------------------------------------------------

            case "stop":

                stopUser(ws);

                break;


            // ------------------------------------------------
            // REPORT
            // ------------------------------------------------

            case "report":

                reportUser(
                    ws,
                    data
                );

                break;


            // ------------------------------------------------
            // BLOCK
            // ------------------------------------------------

            case "block":

                blockUser(ws);

                break;
        }
    });


    // ========================================================
    // CLOSE
    // ========================================================

    ws.on("close", () => {

        const user = users.get(ws);

        console.log(
            "Disconnected:",
            user ? user.id : "unknown"
        );

        removeFromQueue(ws);

        if (user) {

            disconnectFromPartner(
                ws,
                "disconnected"
            );
        }

        users.delete(ws);

        broadcastActiveUsers();
    });


    // ========================================================
    // ERROR
    // ========================================================

    ws.on("error", (error) => {

        console.log(
            "WebSocket error:",
            error.message
        );
    });
});


// ============================================================
// SERVER
// ============================================================

server.listen(
    PORT,
    "0.0.0.0",
    () => {

        console.log(
            `ChitChat server running on port ${PORT}`
        );
    }
);