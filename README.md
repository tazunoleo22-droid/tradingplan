# Trading Capital Connector

Servidor intermedio para Trading Capital.

## Fase 1
Myfxbook -> este servidor -> app Android.
La contraseña de Myfxbook se guarda solo como variable de entorno del servidor, nunca en el APK.

## Fase 2
Grand Capital MT4 -> EA de solo lectura -> endpoint `/mt4/trade` -> app Android.

## Variables
Copiar `.env.example` a `.env` en desarrollo o configurarlas como secretos en el hosting.

## Ejecutar
```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## Seguridad
Use HTTPS en producción y una `APP_API_KEY` larga.
