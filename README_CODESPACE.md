# MigSock — GitHub Codespaces

Versi ini berasal dari build UI terbaru dan dibuat agar dapat langsung dijalankan di GitHub Codespaces/Linux.

## Jalankan

```bash
cd ~/migsock
chmod +x start-codespace.sh
bash start-codespace.sh
```

Server memakai port `3000` (atau nilai environment `PORT` jika diberikan) dan bind ke `0.0.0.0`, sehingga Codespaces dapat meneruskan port tersebut.

Setelah port 3000 muncul pada tab **PORTS**, buka URL forwarded port tersebut. Jika Codespaces menawarkan **Open in Browser**, pilih itu.

## SAVE / LOAD

Di Codespaces/Linux, konfigurasi disimpan di folder `configs/` di dalam project. Di Termux, folder Download yang tersedia tetap diprioritaskan.

## Jalankan satu perintah

```bash
bash start-codespace.sh
```


## Login/port fix revision
This revision uses one `loginOne()` implementation only. The browser opens exactly one `/ws` connection per WS slot and waits for `auth.required` before sending `developer.login`.

Codespaces is forced to use port `3000` in `start-codespace.sh` and the server binds to `0.0.0.0:3000`.

Diagnostics:
- `GET /health` returns the active port, upstream URL and WebSocket path.
- Terminal logs show `PROXY BROWSER CONNECT`, `UPSTREAM CONNECTED`, or `UPSTREAM CONNECT FAILED`.

Expected login sequence:
`Proxy OPEN` -> `RECV auth.required` -> `LOGIN OK`.

If it stops before `RECV auth.required`, inspect the terminal for `UPSTREAM CONNECT...`.
If it reaches `API ERROR`, the upstream/API response is being received and the error details are shown in the UI log.
