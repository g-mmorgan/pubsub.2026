#!/bin/sh

set -u

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT="${1:-54321}"
TMP_DIR="$(mktemp -d /tmp/pubsub-test.XXXXXX)"

REGISTRY_PID=""
BROKER_PID=""

cleanup() {
    echo
    echo "== Limpieza final =="

    if [ -n "${BROKER_PID}" ]; then
        if kill -0 "${BROKER_PID}" 2>/dev/null; then
            echo "Parando broker..."
            kill "${BROKER_PID}" 2>/dev/null || true
            wait "${BROKER_PID}" 2>/dev/null || true
        fi
    fi

    if [ -n "${REGISTRY_PID}" ]; then
        if kill -0 "${REGISTRY_PID}" 2>/dev/null; then
            echo "Parando rmiregistry..."
            kill "${REGISTRY_PID}" 2>/dev/null || true
            wait "${REGISTRY_PID}" 2>/dev/null || true
        fi
    fi

    echo "Borrando temporales: ${TMP_DIR}"
    rm -rf "${TMP_DIR}"
}

trap cleanup EXIT INT TERM

echo "== Test automático PubSub =="
echo "Directorio raíz: ${ROOT_DIR}"
echo "Puerto RMI:      ${PORT}"
echo

echo "== Comprobando ficheros necesarios =="

# TestAutoGrp.java puede estar en la raíz o ya en su destino final
TESTAUTO_SRC="${ROOT_DIR}/client_node/src/pubsubapps/TestAuto.java"
TESTAUTO_ROOT="${ROOT_DIR}/TestAuto.java"

if [ ! -f "${TESTAUTO_SRC}" ]; then
    if [ -f "${TESTAUTO_ROOT}" ]; then
        echo "Copiando TestAuto.java a client_node/src/pubsubapps/..."
        cp "${TESTAUTO_ROOT}" "${TESTAUTO_SRC}"
    else
        echo "[ERROR] Falta TestAuto.java (buscado en raíz y en client_node/src/pubsubapps/)"
        echo "Guarda TestAuto.java en la raíz del proyecto o en client_node/src/pubsubapps/."
        exit 1
    fi
fi

if [ ! -x "${ROOT_DIR}/compile_all.sh" ]; then
    echo "[ERROR] No encuentro compile_all.sh ejecutable."
    echo "Ejecuta: chmod +x compile_all.sh"
    exit 1
fi

if [ ! -x "${ROOT_DIR}/common/compile.sh" ]; then
    echo "[ERROR] No encuentro common/compile.sh ejecutable."
    echo "Ejecuta: chmod +x common/compile.sh"
    exit 1
fi

if [ ! -x "${ROOT_DIR}/broker_node/compile.sh" ]; then
    echo "[ERROR] No encuentro broker_node/compile.sh ejecutable."
    echo "Ejecuta: chmod +x broker_node/compile.sh"
    exit 1
fi

if [ ! -x "${ROOT_DIR}/client_node/compile.sh" ]; then
    echo "[ERROR] No encuentro client_node/compile.sh ejecutable."
    echo "Ejecuta: chmod +x client_node/compile.sh"
    exit 1
fi

if [ ! -x "${ROOT_DIR}/broker_node/start_rmiregistry.sh" ]; then
    echo "[ERROR] No encuentro broker_node/start_rmiregistry.sh ejecutable."
    echo "Ejecuta: chmod +x broker_node/start_rmiregistry.sh"
    exit 1
fi

if [ ! -x "${ROOT_DIR}/broker_node/execute_broker.sh" ]; then
    echo "[ERROR] No encuentro broker_node/execute_broker.sh ejecutable."
    echo "Ejecuta: chmod +x broker_node/execute_broker.sh"
    exit 1
fi

if [ ! -x "${ROOT_DIR}/client_node/execute.sh" ]; then
    echo "[ERROR] No encuentro client_node/execute.sh ejecutable."
    echo "Ejecuta: chmod +x client_node/execute.sh"
    exit 1
fi

echo "[OK] Ficheros necesarios encontrados"
echo

echo "== Compilando proyecto completo =="
cd "${ROOT_DIR}" || exit 1
./compile_all.sh

if [ $? -ne 0 ]; then
    echo "[ERROR] La compilación general ha fallado."
    exit 1
fi

echo "[OK] Compilación general completada"
echo

echo "== Compilando TestAuto.java =="
cd "${ROOT_DIR}/client_node/src" || exit 1

javac -Xlint -cp .:../common.jar:../bin -d ../bin pubsubapps/TestAutoGrp.java

if [ $? -ne 0 ]; then
    echo "[ERROR] La compilación de TestAuto.java ha fallado."
    exit 1
fi

echo "[OK] TestAuto.java compilado"
echo

echo "== Arrancando RMI Registry =="
cd "${ROOT_DIR}/broker_node" || exit 1

./start_rmiregistry.sh "${PORT}" > "${TMP_DIR}/rmiregistry.log" 2>&1 &
REGISTRY_PID=$!

sleep 1

if ! kill -0 "${REGISTRY_PID}" 2>/dev/null; then
    echo "[ERROR] El rmiregistry no está vivo."
    echo "--- Log rmiregistry ---"
    cat "${TMP_DIR}/rmiregistry.log"
    exit 1
fi

echo "[OK] RMI Registry arrancado con PID ${REGISTRY_PID}"
echo

echo "== Arrancando broker =="
cd "${ROOT_DIR}/broker_node" || exit 1

./execute_broker.sh "${PORT}" > "${TMP_DIR}/broker.log" 2>&1 &
BROKER_PID=$!

sleep 2

if ! kill -0 "${BROKER_PID}" 2>/dev/null; then
    echo "[ERROR] El broker no está vivo."
    echo "--- Log broker ---"
    cat "${TMP_DIR}/broker.log"
    exit 1
fi

echo "[OK] Broker arrancado con PID ${BROKER_PID}"
echo

echo "== Ejecutando TestAuto =="
echo

cd "${ROOT_DIR}/client_node" || exit 1

./execute.sh TestAutoGrp localhost "${PORT}"
TEST_RESULT=$?

echo
echo "== Logs del broker =="
cat "${TMP_DIR}/broker.log"

echo
if [ "${TEST_RESULT}" -eq 0 ]; then
    echo "[OK] Todos los tests automáticos han pasado."
else
    echo "[FAIL] Algún test automático ha fallado."
fi

exit "${TEST_RESULT}"