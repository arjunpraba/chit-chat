const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
    res.writeHead(200, {
        "Content-Type": "application/json"
    });

    res.end(
        JSON.stringify({
            status: "online",
            service: "Stranger Chat"
        })
    );
});

const wss = new WebSocket.Server({
    server: server
});

const waitingUsers = [];
const users = new Map();

function getClientIp(req) {
    const forwarded = req.headers["x-forwarded-for"];

    if (forwarded) {
        return forwarded.split(",")[0].trim();
    }

    return req.socket.remoteAddress || "unknown";
}

function send(ws, data) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }
}

/*
 * Send current active user count to everyone.
 */
function broadcastActiveUsers() {
    const count = users.size;

    users.forEach((user, ws) => {
        send(ws, {
            type: "active_users",
            count: count
        });
    });
}

function removeFromQueue(ws) {
    const index = waitingUsers.indexOf(ws);

    if (index !== -1) {
        waitingUsers.splice(index, 1);
    }
}

function stopTyping(ws) {
    const user = users.get(ws);

    if (!user) {
        return;
    }

    if (!user.typing) {
        return;
    }

    user.typing = false;

    if (user.partner) {
        send(user.partner, {
            type: "typing",
            typing: false
        });
    }
}

function disconnectFromPartner(ws) {
    const user = users.get(ws);

    if (!user || !user.partner) {
        return;
    }

    // Stop typing before disconnecting.
    stopTyping(ws);

    const partner = user.partner;

    user.partner = null;

    if (users.has(partner)) {
        const partnerUser = users.get(partner);

        // Stop partner typing state too.
        partnerUser.partner = null;
        partnerUser.typing = false;

        send(partner, {
            type: "typing",
            typing: false
        });

        send(partner, {
            type: "partner_left"
        });
    }
}

function findStranger(ws) {
    const user = users.get(ws);

    if (!user) {
        return;
    }

    removeFromQueue(ws);

    // Already connected.
    if (user.partner) {
        return;
    }

    while (waitingUsers.length > 0) {
        const stranger = waitingUsers.shift();

        if (
            stranger.readyState !== WebSocket.OPEN ||
            !users.has(stranger)
        ) {
            continue;
        }

        const strangerUser = users.get(stranger);

        if (!strangerUser || stranger === ws) {
            continue;
        }

        // Don't match the same IP.
        if (strangerUser.ip === user.ip) {
            continue;
        }

        user.partner = stranger;
        user.searching = false;
        user.typing = false;

        strangerUser.partner = ws;
        strangerUser.searching = false;
        strangerUser.typing = false;

        send(ws, {
            type: "matched"
        });

        send(stranger, {
            type: "matched"
        });

        return;
    }

    waitingUsers.push(ws);

    send(ws, {
        type: "waiting"
    });
}

wss.on("connection", (ws, req) => {

    const ip = getClientIp(req);

    console.log("Connected:", ip);

    users.set(ws, {
        ip: ip,
        partner: null,
        searching: false,
        typing: false
    });

    // Tell newly connected user that connection is ready.
    send(ws, {
        type: "connected"
    });

    // Send current active user count.
    broadcastActiveUsers();

    ws.on("message", (raw) => {

        let data;

        try {
            data = JSON.parse(raw.toString());
        } catch (error) {
            return;
        }

        const user = users.get(ws);

        if (!user) {
            return;
        }

        switch (data.type) {

            // =====================================================
            // START
            // =====================================================

            case "start":

                if (user.partner) {
                    return;
                }

                user.searching = true;

                findStranger(ws);

                break;


            // =====================================================
            // MESSAGE
            // =====================================================

            case "message":

                if (!user.partner) {
                    return;
                }

                const messageText = String(data.text || "").trim();

                if (messageText.length === 0) {
                    return;
                }

                // Stop typing when message is sent.
                stopTyping(ws);

                send(user.partner, {
                    type: "message",
                    text: messageText
                });

                break;


            // =====================================================
            // TYPING
            // =====================================================

            case "typing":

                if (!user.partner) {
                    return;
                }

                const isTyping = data.typing === true;

                user.typing = isTyping;

                send(user.partner, {
                    type: "typing",
                    typing: isTyping
                });

                break;


            // =====================================================
            // SKIP
            // =====================================================

            case "skip":

                stopTyping(ws);

                disconnectFromPartner(ws);

                user.searching = true;

                findStranger(ws);

                break;


            // =====================================================
            // STOP
            // =====================================================

            case "stop":

                stopTyping(ws);

                removeFromQueue(ws);

                disconnectFromPartner(ws);

                user.searching = false;

                send(ws, {
                    type: "stopped"
                });

                break;
        }
    });


    // =============================================================
    // DISCONNECT
    // =============================================================

    ws.on("close", () => {

        console.log("Disconnected:", ip);

        stopTyping(ws);

        removeFromQueue(ws);

        disconnectFromPartner(ws);

        users.delete(ws);

        // Update everyone after disconnect.
        broadcastActiveUsers();
    });
});


// Important for Render.
server.listen(PORT, "0.0.0.0", () => {
    console.log(
        `Stranger Chat server running on port ${PORT}`
    );
});