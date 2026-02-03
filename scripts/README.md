# Scripts

## start-all.ps1
Arranca todo el stack (incluye Ollama) y descarga el modelo si no existe.

### Uso basico
```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-all.ps1
```

### Forzar build
```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-all.ps1 -Build
```

### Usar otro modelo
```powershell
powershell -ExecutionPolicy Bypass -File scripts\start-all.ps1 -Model qwen2.5:7b
```

### Que hace
1. `docker compose up -d ollama`
2. `ollama pull <modelo>` si no existe
3. `docker compose up -d` (con `--build` si usas `-Build`)
