import httpx
import time

from typing import List, Union

from qio.core import (
    QuantumProgram,
    QuantumProgramResult,
    QuantumComputationModel,
    QuantumComputationParameters,
)

from yardstiq.core import (
    provider,
    Provider,
    Backend,
    BackendAvailability,
)

from scaleway_qaas_client.v1alpha1 import (
    QaaSClient,
    QaaSPlatform,
    QaaSPlatformAvailability,
    QaaSJobResult,
)


class ScalewayBackend(Backend):
    def __init__(
        self, provider: "ScalewayProvider", platform: QaaSPlatform, client: QaaSClient
    ):
        super().__init__(
            provider=provider, name=platform.name, version=platform.version
        )

        self.__platform: QaaSPlatform = platform
        self.__client: QaaSClient = client
        self.__session_id: str = None

    def allocate(self, **kwargs) -> None:
        if self.__session_id:
            return

        deduplication_id = kwargs.get("deduplication_id", None)

        session = self.__client.create_session(
            self.__platform.id, deduplication_id=deduplication_id
        )

        self.__session_id = session.id

    def deallocate(self, **kwargs) -> None:
        if self.__session_id:
            return

        self.__client.terminate_session(self.__session_id)
        self.__session_id = None

    def run(
        self,
        program: Union[QuantumProgram, List[QuantumProgram]],
        shots: int,
        **kwargs,
    ) -> List[QuantumProgramResult]:
        if not isinstance(program, list):
            program = [program]

        computation_model_dict = QuantumComputationModel(
            programs=program,
            backend=None,
            client=None,
        ).to_dict()

        computation_parameters_dict = QuantumComputationParameters(
            shots=shots,
        ).to_dict()

        model = self.__client.create_model(computation_model_dict)

        if not model:
            raise RuntimeError("Failed to push model data")

        job = self.__client.create_job(
            self.__session_id, model_id=model.id, parameters=computation_parameters_dict
        )

        while job.status in ["waiting", "running"]:
            time.sleep(2)
            job = self.__client.get_job(job.id)

        if job.status == "error":
            raise RuntimeError(f"Job failed with error: {job.progress_message}")

        job_results = self.__client.list_job_results(job.id)

        program_results = list(
            map(
                lambda r: self._extract_payload_from_response(r),
                job_results,
            )
        )

        if len(program_results) == 1:
            return program_results[0]

        return program_results

    def _extract_payload_from_response(
        self, job_result: QaaSJobResult
    ) -> QuantumProgramResult:
        result = job_result.result

        if result is None or result == "":
            url = job_result.url

            if url is not None:
                resp = httpx.get(url)
                resp.raise_for_status()
                result = resp.text
            else:
                raise RuntimeError("Got result with empty data and url fields")

        return QuantumProgramResult.from_json(result)

    @property
    def max_qubit_count(self) -> int:
        return self.__platform.max_qubit_count

    @property
    def max_shots_per_run(self) -> int:
        return self.__platform.max_shot_count

    @property
    def availability(self) -> BackendAvailability:
        availability_map = {
            QaaSPlatformAvailability.AVAILABLE: BackendAvailability.AVAILABLE,
            QaaSPlatformAvailability.SHORTAGE: BackendAvailability.MAINTENANCE,
            QaaSPlatformAvailability.MAINTENANCE: BackendAvailability.MAINTENANCE,
        }
        return availability_map.get(
            self.__platform.availability, BackendAvailability.UNKNOWN_AVAILABILITY
        )


@provider("scaleway")
class ScalewayProvider(Provider):
    def __init__(self, **kwargs):
        super().__init__("scaleway")

        secret_key = kwargs.get("scaleway_secret_key", None)
        project_id = kwargs.get("scaleway_project_id", None)
        api_url = kwargs.get("scaleway_api_url", None)

        self.__client = QaaSClient(
            project_id=project_id, secret_key=secret_key, url=api_url
        )

    def get_backend(self, name: str) -> ScalewayBackend:
        platforms = self.__client.list_platforms(name=name)

        if not platforms or len(platforms) == 0:
            raise ValueError(f"Backend '{name}' not found in Scaleway providers")

        return self._platform_to_backend(self, platforms[0])

    def list_backends(self) -> List[ScalewayBackend]:
        platforms = self.__client.list_platforms()

        if not platforms:
            raise ValueError(f"Backend not found in Scaleway providers")

        if len(platforms) == 0:
            return []

        return list(map(lambda p: self._platform_to_backend(p), platforms))

    def _platform_to_backend(self, platform: QaaSPlatform) -> ScalewayBackend:
        return ScalewayBackend(provider=self)
