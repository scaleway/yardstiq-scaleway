from typing import Any, Dict, List

from yardstiq.core import provider, Provider, Backend

from scaleway_qaas_client.v1alpha1 import QaaSClient, QaaSPlatform


@provider("scaleway")
class ScalewayProvider(Provider):
    def __init__(self, **kwargs):
        super().__init__("scaleway")

        secret_key = kwargs.get("scaleway_secret_key", None)
        project_id = kwargs.get("scaleway_project_id", None)
        api_url = kwargs.get("scaleway_api_url", None)

        self.__client = QaaSClient(project_id=project_id, secret_key=secret_key, url=api_url)

    def get_backend(self, name : str) -> Backend:
        platforms = self.__client.list_platforms(name=name)

        if not platforms or len(platforms) == 0:
            raise ValueError(f"Backend '{name}' not found in Scaleway providers")

        return self._platform_to_backend(self, platforms[0])

    def list_backends(self) -> List[Backend]:
        platforms = self.__client.list_platforms()

        if not platforms:
            raise ValueError(f"Backend not found in Scaleway providers")

        if len(platforms) == 0:
            return []

        return list(map(lambda p: self._platform_to_backend(p), platforms))

    def _platform_to_backend(self, platform: QaaSPlatform) -> Backend:
        return Backend(provider=self)