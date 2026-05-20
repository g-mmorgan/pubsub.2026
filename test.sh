#!/bin/sh

# script de test automatico para la practica pubsub
# arranca registry + broker, compila TestAutoGrp y ejecuta los tests
# uso: ./test.sh [puerto]   (por defecto 54321)

set -u

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT="${1:-54321}"
TMP_DIR="$(mktemp -d /tmp/pubsub-test.XXXXXX)"

REGISTRY_PID=""
BROKER_PID=""

# limpieza al salir por cualquier motivo (exit normal, ctrl-c, señal)
cleanup() {
    echo
    echo "== limpieza final =="

    if [ -n "${BROKER_PID}" ]; then
        if kill -0 "${BROKER_PID}" 2>/dev/null; then
            echo "parando broker (PID ${BROKER_PID})..."
            kill "${BROKER_PID}" 2>/dev/null || true
            wait "${BROKER_PID}" 2>/dev/null || true
        fi
    fi

    if [ -n "${REGISTRY_PID}" ]; then
        if kill -0 "${REGISTRY_PID}" 2>/dev/null; then
            echo "parando rmiregistry (PID ${REGISTRY_PID})..."
            kill "${REGISTRY_PID}" 2>/dev/null || true
            wait "${REGISTRY_PID}" 2>/dev/null || true
        fi
    fi

    echo "borrando temporales: ${TMP_DIR}"
    rm -rf "${TMP_DIR}"
}

trap cleanup EXIT INT TERM

echo "== test automatico pubsub =="
echo "directorio raiz: ${ROOT_DIR}"
echo "puerto RMI:      ${PORT}"
echo

# ---------------------------------------------------------------
# comprobacion de ficheros y permisos necesarios antes de empezar
# ---------------------------------------------------------------
echo "== comprobando ficheros necesarios =="

# el fichero java del test debe estar en su ubicacion final dentro del proyecto
TESTAUTO_SRC="${ROOT_DIR}/client_node/src/pubsubapps/TestAutoGrp.java"
TESTAUTO_ROOT="${ROOT_DIR}/TestAutoGrp.java"

if [ ! -f "${TESTAUTO_SRC}" ]; then
    # si esta en la raiz del proyecto, lo copiamos al lugar correcto
    if [ -f "${TESTAUTO_ROOT}" ]; then
        echo "copiando TestAutoGrp.java a client_node/src/pubsubapps/..."
        cp "${TESTAUTO_ROOT}" "${TESTAUTO_SRC}"
    else
        echo "[ERROR] falta TestAutoGrp.java"
        echo "  buscado en: ${TESTAUTO_SRC}"
        echo "  buscado en: ${TESTAUTO_ROOT}"
        echo "  coloca TestAutoGrp.java en la raiz del proyecto o en client_node/src/pubsubapps/"
        exit 1
    fi
fi

# comprobamos que todos los scripts del proyecto existen y tienen permiso de ejecucion
check_exec() {
    if [ ! -x "$1" ]; then
        echo "[ERROR] no encuentro $1 ejecutable"
        echo "  ejecuta: chmod +x $1"
        exit 1
    fi
}

check_exec "${ROOT_DIR}/compile_all.sh"
check_exec "${ROOT_DIR}/common/compile.sh"
check_exec "${ROOT_DIR}/broker_node/compile.sh"
check_exec "${ROOT_DIR}/client_node/compile.sh"
check_exec "${ROOT_DIR}/broker_node/start_rmiregistry.sh"
check_exec "${ROOT_DIR}/broker_node/execute_broker.sh"
check_exec "${ROOT_DIR}/client_node/execute.sh"

echo "[OK] ficheros necesarios encontrados"
echo

# ---------------------------------------------------------------
# compilacion del proyecto completo
# ---------------------------------------------------------------
echo "== compilando proyecto completo =="
cd "${ROOT_DIR}" || exit 1
./compile_all.sh

if [ $? -ne 0 ]; then
    echo "[ERROR] la compilacion general ha fallado"
    exit 1
fi

echo "[OK] compilacion general completada"
echo

# compilamos TestAutoGrp por separado porque no forma parte del compile_all.sh
echo "== compilando TestAutoGrp.java =="
cd "${ROOT_DIR}/client_node/src" || exit 1

javac -Xlint -cp .:../common.jar:../bin -d ../bin pubsubapps/TestAutoGrp.java

if [ $? -ne 0 ]; then
    echo "[ERROR] la compilacion de TestAutoGrp.java ha fallado"
    exit 1
fi

echo "[OK] TestAutoGrp.java compilado"
echo

# ---------------------------------------------------------------
# arranque del registry RMI
# ---------------------------------------------------------------
echo "== arrancando RMI registry =="
cd "${ROOT_DIR}/broker_node" || exit 1

./start_rmiregistry.sh "${PORT}" > "${TMP_DIR}/rmiregistry.log" 2>&1 &
REGISTRY_PID=$!

# esperamos un momento a que el registry levante
sleep 2

if ! kill -0 "${REGISTRY_PID}" 2>/dev/null; then
    echo "[ERROR] el rmiregistry no esta vivo"
    echo "--- log rmiregistry ---"
    cat "${TMP_DIR}/rmiregistry.log"
    exit 1
fi

echo "[OK] RMI registry arrancado con PID ${REGISTRY_PID}"
echo

# ---------------------------------------------------------------
# arranque del broker
# ---------------------------------------------------------------
echo "== arrancando broker =="
cd "${ROOT_DIR}/broker_node" || exit 1

./execute_broker.sh "${PORT}" > "${TMP_DIR}/broker.log" 2>&1 &
BROKER_PID=$!

# el broker necesita un poco mas de tiempo para registrarse en el registry
sleep 3

if ! kill -0 "${BROKER_PID}" 2>/dev/null; then
    echo "[ERROR] el broker no esta vivo"
    echo "--- log broker ---"
    cat "${TMP_DIR}/broker.log"
    exit 1
fi

echo "[OK] broker arrancado con PID ${BROKER_PID}"
echo

# ---------------------------------------------------------------
# ejecucion del test automatico
# ---------------------------------------------------------------
echo "== ejecutando TestAutoGrp =="
echo

cd "${ROOT_DIR}/client_node" || exit 1

# pasamos host y puerto al test java
./execute.sh TestAutoGrp localhost "${PORT}"
TEST_RESULT=$?

# mostramos el log del broker por si hay excepciones en el servidor
echo
echo "== logs del broker =="
cat "${TMP_DIR}/broker.log"

echo
if [ "${TEST_RESULT}" -eq 0 ]; then
    echo "[OK] todos los tests automaticos han pasado"
else
    echo "[FAIL] algun test automatico ha fallado (codigo de salida: ${TEST_RESULT})"
fi

exit "${TEST_RESULT}"