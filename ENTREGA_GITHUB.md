# Cómo subir el proyecto a GitHub

## 1. Revisar archivos que no deben subirse

El dataset no debe subirse porque pesa cerca de 906 MB. Ya está ignorado por `.gitignore`:

```text
app/data/*.csv.gz
```

Los resultados Kafka sí son livianos y conviene subirlos porque respaldan el informe:

```text
experiment_results_kafka/
```

## 2. Crear repositorio en GitHub

En GitHub:

1. Ir a <https://github.com/new>
2. Crear un repositorio, por ejemplo `tarea-2-sistemas-distribuidos`
3. No marcar "Add README", porque el proyecto ya tiene uno.

## 3. Conectar el repositorio local

Si tu repo remoto actual todavía apunta a la Tarea 1, revisa:

```bash
git remote -v
```

Si debes cambiarlo al nuevo repositorio:

```bash
git remote set-url origin https://github.com/TU_USUARIO/tarea-2-sistemas-distribuidos.git
```

Si no existe remote:

```bash
git remote add origin https://github.com/TU_USUARIO/tarea-2-sistemas-distribuidos.git
```

## 4. Hacer commits separados

Para que la rúbrica no lo vea como "todo subido de una sola vez", conviene separar commits:

```bash
git add app/kafka_common.py app/kafka_producer.py app/kafka_consumer.py app/kafka_metrics.py app/kafka_monitor.py app/kafka_report.py app/traffic_generator.py app/requirements.txt docker-compose.yml
git commit -m "Agrega arquitectura Kafka con producer consumers retry y DLQ"
```

```bash
git add run_kafka_experiments.py experiment_results_kafka/
git commit -m "Agrega experimentos y resultados de evaluacion Kafka"
```

```bash
git add README.md informe_tarea2.tex VIDEO_DEMO_GUIA.md ENTREGA_GITHUB.md
git commit -m "Documenta ejecucion informe y guia de demostracion"
```

## 5. Subir a GitHub

```bash
git push -u origin main
```

Si GitHub pide login, usa un Personal Access Token en lugar de contraseña.

## 6. Compilar informe PDF

Como esta máquina no tiene `pdflatex`, la forma más simple es Overleaf:

1. Entrar a <https://www.overleaf.com>
2. Crear proyecto nuevo.
3. Subir `informe_tarea2.tex`.
4. Subir la carpeta `experiment_results_kafka/` con los PNG.
5. Compilar.
6. Descargar el PDF final.

El PDF es el archivo que se sube a Canvas.

## 7. Video

Graba siguiendo `VIDEO_DEMO_GUIA.md`. Sube el video a YouTube o Drive y entrega un enlace, no un `.mp4` descargable.
