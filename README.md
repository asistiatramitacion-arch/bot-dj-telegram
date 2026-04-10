# DJ-PLAN Railway v3

Proyecto listo para subir a GitHub y desplegar en Railway.

## Qué hace

- Fija **un solo panel DJ arriba**.
- Muestra en el panel:
  - estado del directo
  - DJ asignado
  - cola actual
  - biblioteca
  - canción sonando ahora
- Reproduce la música en el **voice chat** del grupo usando **userbot + PyTgCalls**.
- **No busca música por sí mismo**.
- La búsqueda se hace con tu otro bot de música usando el texto configurado en `SEARCH_TRIGGER`.
- Cuando aparece una **canción descargada en el chat**, DJ-PLAN la detecta y pone debajo:
  - `▶️ Voice ahora`
  - `➕ Cola`
  - `📚 Biblioteca`
- Si respondes a una canción del chat escribiendo `Dj plan`, también saca esos botones.
- Permite:
  - ver la lista actual
  - ver la biblioteca
  - guardar la lista actual con nombre
  - cargar listas guardadas
  - siguiente / anterior
  - pausa / reanudar
  - cerrar la sesión DJ

## Flujo real de uso

1. Entras al grupo.
2. Ejecutas `/start`.
3. Pulsas `🎛️ Abrir panel DJ`.
4. Activas el modo DJ.
5. Te asignas DJ con `🎤 DJ = yo` o con `/dj` respondiendo a un usuario.
6. Buscas música con tu otro bot escribiendo en el chat algo como:
   - `@sha nombre de la canción`
7. Cuando aparezca la canción en el grupo, DJ-PLAN la detectará y pondrá los botones debajo.
8. Solo el DJ asignado podrá pulsar:
   - `▶️ Voice ahora`
   - `➕ Cola`
   - `📚 Biblioteca`

## Botones del panel

- `📋 Lista actual`: ver la cola y mover / borrar / reproducir.
- `📚 Biblioteca`: canciones guardadas para reutilizar.
- `💾 Guardar lista`: guarda la cola actual con un nombre.
- `📂 Cargar lista`: recupera listas guardadas.
- `🔎 Buscar música`: muestra el mensaje guía para buscar con tu otro bot.
- `⏯️ Pausa/Reanudar`: controla el voice.
- `⏮️ Anterior`: vuelve a la canción anterior si existe.
- `⏭️ Siguiente`: salta a la siguiente canción de la cola.
- `🎧 Entrar al voice`: abre el enlace del voice si configuras `VOICE_CHAT_LINK`.
- `🎤 DJ = yo`: te asigna como DJ si eres admin.
- `🟢 DJ ON / 🔴 DJ OFF`: activa o apaga el modo DJ.
- `🔄 Actualizar`: refresca el panel y limpia menús temporales.
- `❌ Cerrar`: apaga el modo DJ, limpia cola, desmonta el voice y quita el panel fijado.

## Variables de entorno en Railway

Copia `.env.example` y pon esto en Railway:

- `BOT_TOKEN`
- `API_ID`
- `API_HASH`
- `USERBOT_SESSION`
- `ADMIN_IDS`
- `VOICE_CHAT_LINK` (opcional)
- `SEARCH_TRIGGER` (por ejemplo `@sha `)

## Cómo sacar USERBOT_SESSION

1. Crea antes tu sesión local con Telethon.
2. Ejecuta:

```bash
python make_string_session.py
```

3. Copia la cadena que te devuelva y pégala en Railway como `USERBOT_SESSION`.

## Requisitos del grupo

La cuenta usada como userbot debe:
- estar dentro del grupo
- ser administradora
- poder gestionar chat de voz
- poder borrar mensajes

## Subida a GitHub y Railway

1. Sube todos estos archivos a un repositorio de GitHub.
2. En Railway crea un proyecto desde ese repo.
3. Railway detectará el `Dockerfile`.
4. Añade las variables de entorno.
5. Despliega.

## Archivos

- `main.py`: bot principal + userbot + panel DJ + cola + biblioteca.
- `Dockerfile`: imagen lista para Railway con `ffmpeg`.
- `requirements.txt`: dependencias Python.
- `.env.example`: plantilla de variables.
- `make_string_session.py`: convierte tu sesión local a `StringSession`.

## Notas

- El bot **no añade botones al mensaje de otro bot**; lo que hace es responder justo debajo con un mensaje de control. Es la forma correcta de hacerlo en Telegram.
- El panel fijado es **uno solo** y se edita.
- Los menús temporales se limpian con `Actualizar` y también se autodestruyen con el tiempo.
