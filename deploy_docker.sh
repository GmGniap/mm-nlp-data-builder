#!/usr/bin/env bash
# =============================================================================
# deploy_docker.sh — Dynamic Docker Build & Push Script for CNCF / OCI Registry
# =============================================================================
set -euo pipefail

# -----------------------------------------------------------------------------
# Color output helpers
# -----------------------------------------------------------------------------
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# -----------------------------------------------------------------------------
# Locate project root and load .env if available
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}"

if [ -f "${ROOT_DIR}/.env" ]; then
  set -a
  # shellcheck source=/dev/null
  source "${ROOT_DIR}/.env"
  set +a
fi

# -----------------------------------------------------------------------------
# Default Configuration (Override via .env, env vars, or CLI flags)
# -----------------------------------------------------------------------------
REGISTRY="${DOCKER_REGISTRY:-${REGISTRY:-your-registry.example.com:5005}}"
IMAGE_NAME="${DOCKER_IMAGE_NAME:-${IMAGE_NAME:-telegram_scraper}}"
TAG="${DOCKER_IMAGE_TAG:-${TAG:-v0.1}}"
DOCKERFILE_PATH="${DOCKERFILE_PATH:-services/telegram_scraper/Dockerfile}"
BUILD_CONTEXT="${DOCKER_CONTEXT:-${ROOT_DIR}}"
PLATFORM="${DOCKER_PLATFORM:-${PLATFORM:-linux/arm64}}"
PUSH_IMAGE=true
TAG_LATEST=false

# -----------------------------------------------------------------------------
# CLI Argument Parser & Help
# -----------------------------------------------------------------------------
show_help() {
  cat << EOF
Usage: $(basename "$0") [OPTIONS]

Build and push container images to a self-hosted CNCF/OCI Registry.

Options:
  -r, --registry REGISTRY    Target registry host & port (default: ${REGISTRY})
  -i, --image NAME           Image name (default: ${IMAGE_NAME})
  -t, --tag TAG              Image tag/version (default: ${TAG})
  -f, --file DOCKERFILE      Path to Dockerfile (default: ${DOCKERFILE_PATH})
  -c, --context DIR          Docker build context directory (default: project root)
  -p, --platform PLATFORM    Target architecture (default: ${PLATFORM})
  -l, --latest               Also tag and push as ':latest'
      --no-push              Build image only, skip pushing to registry
  -h, --help                 Display this help message and exit

Environment Variables (or in .env):
  DOCKER_REGISTRY            Registry URL (e.g. your-registry.example.com:5005)
  DOCKER_IMAGE_NAME          Image name (e.g. telegram-scraper)
  DOCKER_IMAGE_TAG           Tag version (e.g. v0.1)
  DOCKERFILE_PATH            Path to Dockerfile (e.g. services/telegram_scraper/Dockerfile)
  DOCKER_PLATFORM            Platform (e.g. linux/arm64)

Examples:
  ./deploy_docker.sh
  ./deploy_docker.sh -t v0.2 --latest
  ./deploy_docker.sh -i custom-scraper -t 1.0.0 -f services/telegram_scraper/Dockerfile
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -r|--registry)
      REGISTRY="$2"
      shift 2
      ;;
    -i|--image)
      IMAGE_NAME="$2"
      shift 2
      ;;
    -t|--tag)
      TAG="$2"
      shift 2
      ;;
    -f|--file)
      DOCKERFILE_PATH="$2"
      shift 2
      ;;
    -c|--context)
      BUILD_CONTEXT="$2"
      shift 2
      ;;
    -p|--platform)
      PLATFORM="$2"
      shift 2
      ;;
    -l|--latest)
      TAG_LATEST=true
      shift
      ;;
    --no-push)
      PUSH_IMAGE=false
      shift
      ;;
    -h|--help)
      show_help
      exit 0
      ;;
    *)
      echo -e "${RED}❌ Unknown argument: $1${NC}" >&2
      show_help
      exit 1
      ;;
  esac
done

# -----------------------------------------------------------------------------
# Resolve Paths & Validate Pre-requisites
# -----------------------------------------------------------------------------
# If DOCKERFILE_PATH is relative and not found directly, check relative to ROOT_DIR
if [[ ! -f "${DOCKERFILE_PATH}" && -f "${ROOT_DIR}/${DOCKERFILE_PATH}" ]]; then
  DOCKERFILE_PATH="${ROOT_DIR}/${DOCKERFILE_PATH}"
fi

if [[ ! -f "${DOCKERFILE_PATH}" ]]; then
  echo -e "${RED}❌ Error: Dockerfile not found at: ${DOCKERFILE_PATH}${NC}" >&2
  exit 1
fi

if ! command -v docker &> /dev/null; then
  echo -e "${RED}❌ Error: 'docker' CLI is not installed or not in PATH.${NC}" >&2
  exit 1
fi

FULL_TAGGED_IMAGE="${REGISTRY}/${IMAGE_NAME}:${TAG}"
LATEST_TAGGED_IMAGE="${REGISTRY}/${IMAGE_NAME}:latest"

echo -e "${BLUE}======================================================${NC}"
echo -e "${BLUE}  🚀 CNCF Registry Deployment for Telegram Scraper   ${NC}"
echo -e "${BLUE}======================================================${NC}"
echo -e "  • Registry    : ${YELLOW}${REGISTRY}${NC}"
echo -e "  • Image Name  : ${YELLOW}${IMAGE_NAME}${NC}"
echo -e "  • Tag Version : ${YELLOW}${TAG}${NC}"
echo -e "  • Dockerfile  : ${YELLOW}${DOCKERFILE_PATH}${NC}"
echo -e "  • Context     : ${YELLOW}${BUILD_CONTEXT}${NC}"
echo -e "  • Platform    : ${YELLOW}${PLATFORM}${NC}"
echo -e "  • Target URI  : ${GREEN}${FULL_TAGGED_IMAGE}${NC}"
echo -e "${BLUE}======================================================${NC}"

# -----------------------------------------------------------------------------
# 1. Build Docker Image
# -----------------------------------------------------------------------------
echo -e "\n${BLUE}==> [1/2] Building Docker image (${PLATFORM})...${NC}"

BUILD_ARGS=("-f" "${DOCKERFILE_PATH}" "--platform" "${PLATFORM}" "-t" "${FULL_TAGGED_IMAGE}")

if [ "${TAG_LATEST}" = true ] && [ "${TAG}" != "latest" ]; then
  BUILD_ARGS+=("-t" "${LATEST_TAGGED_IMAGE}")
fi

docker build "${BUILD_ARGS[@]}" "${BUILD_CONTEXT}"

echo -e "${GREEN}✓ Successfully built ${FULL_TAGGED_IMAGE}${NC}"

# -----------------------------------------------------------------------------
# 2. Push Image to CNCF / OCI Registry
# -----------------------------------------------------------------------------
if [ "${PUSH_IMAGE}" = true ]; then
  echo -e "\n${BLUE}==> [2/2] Pushing image to CNCF registry (${REGISTRY})...${NC}"
  docker push "${FULL_TAGGED_IMAGE}"

  if [ "${TAG_LATEST}" = true ] && [ "${TAG}" != "latest" ]; then
    echo -e "${BLUE}==> Pushing ':latest' tag...${NC}"
    docker push "${LATEST_TAGGED_IMAGE}"
  fi

  echo -e "\n${GREEN}======================================================${NC}"
  echo -e "${GREEN}  🎉 Deployment to Registry Complete!${NC}"
  echo -e "${GREEN}======================================================${NC}"
  echo -e "  Pull command on Airflow / Kubernetes node:"
  echo -e "  ${YELLOW}docker pull ${FULL_TAGGED_IMAGE}${NC}"
  echo -e "${GREEN}======================================================${NC}"
else
  echo -e "\n${YELLOW}ℹ️  Skipping push (--no-push flag active).${NC}"
fi