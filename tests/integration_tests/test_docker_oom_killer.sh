# --memory-swap sets the total memory available, setting it equal to the total memory disables swap
#
# note that cgroups have to be enabled for docker to prevent it from using the host' swap memory:
#   https://fabianlee.org/2020/01/18/docker-placing-limits-on-container-memory-using-cgroups/

docker run --rm -it \
    -m 100M \
    --memory-swap 100M \
    -v $(pwd)/../../:/job_pool \
    -w /job_pool \
    python:3.9-slim \
    python -m tests.integration_tests.test_docker_oom_killer