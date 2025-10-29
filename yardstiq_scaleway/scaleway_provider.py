from typing import Any, Dict, List

from yardstiq.core import (
    provider,
    Provider,
    Backend,
    BackendRunResult,
    BackendAvailability,
    ComputationalModel,
)

from scaleway_qaas_client.v1alpha1 import (
    QaaSClient,
    QaaSPlatform,
    QaaSPlatformAvailability,
    QaaS,
)


class ScalewayBackend(Backend):
    def __init__(
        self, provider: "ScalewayProvider", platform: QaaSPlatform, client: QaaSClient
    ):
        super().__init__(provider=provider, name=platform.name)

        self.__platform: QaaSPlatform = platform
        self.__client: QaaSClient = client
        self.__session_id: str = None

    def allocate(self, **kwargs) -> None:
        deduplication_id = kwargs.get("deduplication_id", None)
        session = self.__client.create_session(
            self.__platform.id, deduplication_id=deduplication_id
        )
        self.__session_id = session.id

    def deallocate(self, **kwargs) -> None:
        self.__client.terminate_session(self.__session_id)

    def run(self, model: ComputationalModel, **kwargs) -> BackendRunResult:
        model = self.__client.create_model(model)
        self.__client.create_job(self.__session_id, model_id=model.id)

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
            self.__platform.availability, BackendAvailability.UNKOWN_AVAILABILITY
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
