# migsock - Termux

Versi ini:
- LOGIN ALL / LOGOUT ALL
- ENTER ALL / LEAVE ALL
- memakai proxy lokal WebSocket ke `wss://developer.mig33.id/developer/ws`
- mengirim `developer.login` setelah `auth.required`
- mengirim JSON `ping` setiap 40 detik setelah login
- Countdown hanya dipicu event `room.kick.state` dengan pesan `A vote to kick`
- LIVE KICK PROGRESS dilaporkan terpisah untuk setiap WebSocket berdasarkan hasil dispatch backend
- setiap laporan WebSocket menampilkan progress, OK, FAIL, target terakhir, dan loop terakhir
- pilihan BRUTE tersedia dari `BRUTE1` sampai `BRUTE10`
- panel log/API log dan tombol CLEAR LOG dihapus

Jalankan:
```bash
chmod +x start.sh stop.sh
bash start.sh
```

Buka:
`http://127.0.0.1:3000`


## Save / Load ke Download HP
- SAVE memakai nama dari textbox SaveLoad dan menyimpan file JSON langsung ke folder Download Termux.
- LOAD mencari file JSON dengan nama tersebut di folder Download Termux secara langsung.
- Jalankan `termux-setup-storage` satu kali jika akses shared storage belum aktif.
- Format file: `<nama SaveLoad>.json`.
- Saldo pada Login Troop hanya menampilkan angka saldo.


## SETTROOP
- Selalu menghasilkan tepat 10 ID.
- Awal/Akhir boleh memakai angka dengan nol depan, misalnya 01-10.
- Untuk Awal 11, rentang 10 ID yang valid adalah 11-20; 21-30, dst.
- Rentang 11-99 penuh berisi 89 angka, sehingga tidak dipakai sebagai satu SETTROOP karena targetnya wajib tepat 10 ID.
- SaveLoad default: multi1.
- SAVE mengunduh JSON ke perangkat; LOAD membuka file picker perangkat.

## SETTROOP pola 00-99
- NameTroop `tai`, Awal `00`, Akhir `99` menghasilkan tepat 10 akun:
  `tai00`, `tai11`, `tai22`, `tai33`, `tai44`, `tai55`, `tai66`, `tai77`, `tai88`, `tai99`.
- Rentang normal tetap menghasilkan tepat 10 akun, misalnya `01-10`, `11-20`, `91-100`.
- SaveLoad default `multi1`.
