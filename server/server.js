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
    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }
}

function removeFromQueue(ws) {
    const index = waitingUsers.indexOf(ws);

    if (index !== -1) {
        waitingUsers.splice(index, 1);
    }
}

function findUser(ws) {
    return users.get(ws);
}

function disconnectFromPartner(ws) {
    const user = users.get(ws);

    if (!user || !user.partner) {
        return;
    }

    const partner = user.partner;

    user.partner = null;

    if (users.has(partner)) {
        const partnerUser = users.get(partner);

        partnerUser.partner = null;

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

    // Already connected
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
        strangerUser.partner = ws;

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
        searching: false
    });

    send(ws, {
        type: "connected"
    });

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

            case "start":

                if (user.partner) {
                    return;
                }

                user.searching = true;

                findStranger(ws);

                break;

            case "message":

                if (!user.partner) {
                    return;
                }

                send(user.partner, {
                    type: "message",
                    text: String(data.text || "")
                });

                break;

            case "skip":

                // Remove current connection.
                disconnectFromPartner(ws);

                user.searching = true;

                // Automatically search again.
                findStranger(ws);

                break;

            case "stop":

                removeFromQueue(ws);

                disconnectFromPartner(ws);

                user.searching = false;

                send(ws, {
                    type: "stopped"
                });

                break;
        }
    });

    ws.on("close", () => {
        console.log("Disconnected:", ip);

        removeFromQueue(ws);

        disconnectFromPartner(ws);

        users.delete(ws);
    });
});

server.listen(PORT, () => {
    console.log(`Stranger Chat server running on port ${PORT}`);
});