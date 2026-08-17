# نشر سيرفر Pocket Option على سيرفر سحابي

## الملفات
- `server_scae.py` — السيرفر (WebSocket)
- `index.html` — صفحة الواجهة للاتصال بالسيرفر من المتصفح
- `requirements.txt` — المكتبة المطلوبة من PyPI
- `Procfile` — لأمر التشغيل (Render / Railway / Heroku)
- `Dockerfile` — للنشر على أي VPS يدعم Docker

## المكتبة المطلوبة (على GitHub)
المكتبة الوحيدة المستخدمة في السيرفر هي **websockets**:
- المستودع: https://github.com/python-websockets/websockets
- على PyPI: https://pypi.org/project/websockets/
- التثبيت: `pip install websockets`

---

## الخيار 1: Render.com (الأسهل، مجاني للبداية)
1. ارفع الملفات إلى مستودع GitHub جديد (server_scae.py, requirements.txt, Procfile).
2. من https://render.com اختر **New → Web Service**.
3. اربط المستودع، واختر:
   - Environment: **Python**
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `python server_scae.py`
4. Render يوفر متغير البيئة `PORT` تلقائيًا (السيرفر يقرأه فعليًا).
5. بعد النشر ستحصل على رابط مثل: `your-app.onrender.com`
   - رابط الاتصال من الصفحة سيكون: `wss://your-app.onrender.com`

## الخيار 2: Railway.app
1. ارفع المشروع لمستودع GitHub.
2. من https://railway.app اختر **New Project → Deploy from GitHub repo**.
3. Railway يكتشف `Procfile` تلقائيًا ويشغّل السيرفر.
4. فعّل **Networking → Generate Domain** للحصول على رابط `wss://`.

## الخيار 3: VPS عادي (DigitalOcean / Hetzner / Contabo...) عبر Docker
```bash
# على السيرفر
git clone <رابط-مستودعك>
cd po_deploy
docker build -t po-server .
docker run -d --restart unless-stopped -p 8765:8765 -e PORT=8765 po-server
```
بعدها الرابط سيكون: `ws://IP_السيرفر:8765`
**يُفضّل وضع Nginx أمامه مع شهادة SSL (Let's Encrypt) للحصول على `wss://` آمن.**

مثال إعداد Nginx كـ reverse proxy:
```nginx
server {
    listen 443 ssl;
    server_name yourdomain.com;

    location / {
        proxy_pass http://127.0.0.1:8765;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
    }
}
```

## الخيار 4: VPS بدون Docker (systemd)
```bash
sudo apt update && sudo apt install python3-pip -y
pip install -r requirements.txt --break-system-packages
```
أنشئ خدمة systemd في `/etc/systemd/system/po-server.service`:
```ini
[Unit]
Description=Pocket Option WS Server
After=network.target

[Service]
WorkingDirectory=/root/po_deploy
ExecStart=/usr/bin/python3 server_scae.py
Restart=always
Environment=PORT=8765

[Install]
WantedBy=multi-user.target
```
```bash
sudo systemctl enable --now po-server
```

---

## استخدام صفحة index.html
1. افتح `index.html` في المتصفح (أو استضفها على أي استضافة ثابتة: GitHub Pages, Netlify, Vercel...).
2. في حقل "رابط السيرفر" ضع رابط الـ WebSocket الذي حصلت عليه من خطوة النشر (`wss://...`).
3. الصق SSID الخاص بجلستك في Pocket Option واضغط "اتصال".

## ملاحظة أمان مهمة
- **SSID هو مفتاح جلستك في حسابك** على Pocket Option — لا تشاركه مع أحد ولا تضعه في مستودع GitHub عام.
- إذا استضفت `index.html` بشكل عام، أي شخص يدخل عليها ويكتب SSID الخاص بك سيتمكن من الوصول لحسابك عبر السيرفر — تأكد من حماية الصفحة (مثلاً بكلمة مرور أو الاستضافة الخاصة فقط).
