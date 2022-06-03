"""
Uploader Class that talks to the Cloud
"""

import logging
from typing import (
                    List,
                    Optional,
                    Type,
                    cast,
                )
import csv
import argparse

from coral.exceptions import CoralUnexpectedExceptionError
from cloud.validationexception import ValidationException
from cloud import CloudSession

from common_mode.framework.cloud import (
                                        CloudMixin
                                        CloudException
                                        CloudCSVProcessorBase
                                        TelemetryEntry
                                        DimensionValues
                                        Dimension
                                        )

DEFAULT_USER = "dev-system"

class CloudCSVUploader(CloudMixin):
    #Class to upload CSV data to Cloud
    def __init__(self,
                 *args,  #pylint: disable=unused-argument
                 cloud_session: Optional[CloudSession] = None,
                 processor_cls: Optional[Type[CloudCSVProcessorBase]] = None,
                 **kwargs):
        self.log = logging.getLogger(CLOUD_ROOT_LOGGER).getChild(self.__class__.__name__)
        if cloud_session:
            self._cloud = cloud_session
        else:
            self._cloud = self.get_cloud_session(*args, **kwargs)
        self.settings = self.get_cloud_secrets()
        self.batch_size = batch_limit
        self.entity_id = entity
        if processor_cls:
            self.processor = processor_cls(self.log)
        else:
            self.processor = CloudCSVProcessorBase(self.log)
        self.log.debug("Starting Cloud CSV Uploader")

    def dispatch_cloud_batch(self,
                             telemetry_batch: List[TelemetryEntry],
                             dimensions: DimensionValues,
                             ) -> Optional[str]:
        """
        Transmit telemetry packets to cloud,
        Checks and adds missing actor name and type b/f uploading
        Returns: Request ID of telemetry request
        Raises: CloudException if a metric key is missing
        """
        self.log.debug(f"Formatting batch {telemetry_batch}")
        for datum in telemetry_batch:
            if "ActorType" not in datum:
                datum[TELEMETRY_ACTOR_TYPE] = self.settings.actor_type
            if "ActorName" not in datum:
                datum[TELEMETRY_ACTOR_NAME] = self.settings.actor_name
        if "session_id" not in dimensions:
            dimensions["session_id"] = self.settings.session_id

        cloud_dimensions = [Dimension(Name=name, Value=value)
                            for name, value in dimensions.items()]

        assert isinstance(telemetry_batch, list)
        assert isinstance(cloud_dimensions, list)

        self.log.debug(f"Sending batch {telemetry_batch} with dimensions {cloud_dimensions}")
        try:
            request_id = self._cloud.put_telemetry(self.entity_id,
                                                 self.settings.namespace,
                                                 telemetry_batch,
                                                 cloud_dimensions)
            self.log.info(f"Cloud upload success. Request id {request_id}")
            return cast(str, request_id)
        except ValueError as err:
            raise CloudException("Error while uploading to cloud") from err
        except ValidationException as err:
            self.log.error(f"Cloud upload failed. {err.message}")
            return None
        except CoralUnexpectedExceptionError:
            raise
        except Exception as err:
            #make sure unknown exception is captured before we leave
            self.log.exception(err)
            raise