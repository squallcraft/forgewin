# Configuración de Mercado Pago - ForgeWin

## Requisitos

- Cuenta de vendedor en [Mercado Pago](https://www.mercadopago.com)
- Aplicación creada en [Tus integraciones](https://www.mercadopago.com.ar/developers/panel/app)
- Access Token (Producción o Pruebas)

## Variables de entorno (.env)

```env
# Mercado Pago - Access Token de tu aplicación
MERCADOPAGO_ACCESS_TOKEN=APP_USR-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

# URL base de ForgeWin (para redirección tras el pago)
FORGEWIN_BASE_URL=https://tu-dominio.com
# En local: http://localhost:8501

# Webhook (opcional, para recibir notificaciones automáticas)
# Debe ser una URL pública (ngrok en desarrollo)
MP_WEBHOOK_URL=https://tu-dominio.com/webhooks/mercadopago
```

## Productos configurados

### Packs de créditos (pago único) — sin planes mensuales

| Pack       | Créditos | Tier      | Precio (CLP) |
|------------|----------|-----------|--------------|
| base       | 20       | base      | 9.990 + IVA  |
| plus       | 50       | plus      | 17.990 + IVA|
| plus_plus  | 100      | plus_plus | 25.990 + IVA|

## Uso desde código

```python
from payment_controller import create_preference_for_pack, list_packs

# Pack de créditos (base, plus, plus_plus)
init_point, error = create_preference_for_pack(
    user_id=1,
    pack_id="plus",
    payer_email="usuario@ejemplo.com",
)
if init_point:
    # Redirigir al usuario a init_point
    pass
```

## Webhooks — Paso a paso completo

Referencia: [MP Docs Webhooks](https://www.mercadopago.cl/developers/es/docs/your-integrations/notifications/webhooks)

### Paso 1: Levantar el servidor de webhooks

```bash
cd /Users/oscarguzman/football-betting-analysis
source venv/bin/activate
uvicorn webhook_server:app --host 0.0.0.0 --port 8001
```

### Paso 2: Exponer URL pública (pruebas)

Para que Mercado Pago pueda enviar notificaciones a tu máquina local, usa ngrok:

```bash
ngrok http 8001
```

Copia la URL HTTPS que te da ngrok (ej: `https://abc123.ngrok-free.app`).

### Paso 3: Configurar en Mercado Pago

1. Entra a [Tus integraciones](https://www.mercadopago.cl/developers/panel/app) o directo: [Webhooks de tu app](https://www.mercadopago.cl/developers/panel/app/4487104805543761/webhooks)
2. Selecciona tu aplicación (ID 4487104805543761)
3. Menú izquierdo → **Webhooks** → **Configurar notificaciones**
4. **URL modo pruebas:** `https://TU-NGROK.ngrok-free.app/webhooks/mercadopago`
5. **URL modo producción:** `https://tu-dominio.com/webhooks/mercadopago` (cuando tengas dominio)
6. **Eventos:** marca **Pagos** (tópico `payment`)
7. Haz clic en **Guardar** → MP generará una **clave secreta** (guárdala por si quieres validar firma después)

### Paso 4: Probar

1. Con el servidor y ngrok activos, realiza un pago de prueba
2. Mercado Pago enviará un POST a tu URL cuando el pago cambie de estado
3. El webhook de ForgeWin consultará el pago en MP y, si está aprobado, sumará créditos al usuario

### Formato de la notificación MP

```json
{
  "type": "payment",
  "data": { "id": "123456789" }
}
```

Nuestro `webhook_server` recibe esto, obtiene el pago con la API de MP y actualiza créditos si `status == approved`.

## Moneda

Por defecto se usa **CLP** (pesos chilenos). Para cambiar, modifica `CREDIT_PACKS` en `payment_controller.py` y ajusta `currency_id` (ARS, USD, etc.).

---

## Configuración de la tienda (panel MP)

1. **Cuenta vendedor** → [Mercado Pago](https://www.mercadopago.cl) o Mercado Libre
2. **Aplicación** → [Tus integraciones](https://www.mercadopago.cl/developers/panel/app) → Crear o seleccionar app
3. **Credenciales** → Panel → Credenciales de prueba (TEST-xxx)
4. **Integración** → Checkout Bricks (ya seleccionada)

---

## Pruebas: flujo correcto

**Importante:** Checkout Bricks no soporta transacciones entre usuarios de prueba. El comprador debe usar un correo común, **no** iniciar sesión con cuenta TESTUSER.

### Pasos para probar

1. **Email en ForgeWin:** `hablemos@e-courier.cl` (cualquiera distinto al de tu cuenta MP)
2. **En el checkout de MP:** No inicies sesión con TESTUSER. Usa el email y paga con tarjeta.
3. **Tarjeta de prueba:** `5031 7557 3453 0604` · venc. `11/30` · CVV `123` · titular `APRO`
4. **Credenciales de la app:** Siempre `TEST-xxxx` (no producción)

### Datos de cuenta comprador (solo para otros flujos)

| Campo | Valor |
|-------|-------|
| ID | 3211824451 |
| Usuario | TESTUSER439350158700344262 |

No usar esta cuenta para pagar en Checkout Bricks; usar email común + tarjeta de prueba.
