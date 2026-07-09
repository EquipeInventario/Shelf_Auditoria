# API Base Ambev - Vercel/FastAPI

API convertida de Azure Functions para FastAPI normal no Vercel.

## Arquivos principais

- `app.py`: API FastAPI pronta para Vercel.
- `requirements.txt`: dependências Python.

## Variáveis de ambiente necessárias no Vercel

Configure em Project Settings > Environment Variables:

```text
DB_HOST=seu_host_mysql
DB_USER=seu_usuario_mysql
DB_PASSWORD=sua_senha_mysql
DB_PORT=3306
DB_NAME_BASE=base_ambev
DB_NAME_INVENTARIO=inventario
```

## Testes após deploy

```text
https://SEU-PROJETO.vercel.app/health
https://SEU-PROJETO.vercel.app/produtos
https://SEU-PROJETO.vercel.app/estoque/sugestoes
```

## Flutter

No build do app, use a URL nova:

```powershell
flutter run -d chrome --dart-define=API_BASE_URL=https://SEU-PROJETO.vercel.app
```

Para APK release:

```powershell
flutter build apk --release --dart-define=API_BASE_URL=https://SEU-PROJETO.vercel.app
```

Não precisa mais usar `AZURE_FUNCTION_KEY`.
