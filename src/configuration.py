import logging
from typing import List, Optional
from pydantic import BaseModel, Field, ValidationError
from keboola.component.exceptions import UserException


class RequiredParameters(BaseModel):
    setupId: str
    outputBucket: str
    daysInterval: int
    hoursInterval: int
    prefixes: List[str] = Field(default=["Click", "Impression", "Trackingpoint", "Event"])


class OverridePKeyItem(BaseModel):
    pkey: List[str]
    dataset: str = Field(default="Click")


class OptionalParameters(BaseModel):
    dateTo: Optional[str]
    override_pkey: Optional[List[OverridePKeyItem]]
    fileCharset: str = Field(default="UTF-8")
    metaFiles: Optional[List[str]]
    alwaysGetMeta: bool = Field(default=True)


class Configuration(BaseModel):
    print_hello: bool
    debug: bool = False
    requiredParameters: RequiredParameters
    optionalParameters: OptionalParameters

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.debug:
            logging.debug("Component will run in Debug mode")
