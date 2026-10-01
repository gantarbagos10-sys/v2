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
