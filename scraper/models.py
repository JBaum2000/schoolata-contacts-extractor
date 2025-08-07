from pydantic import BaseModel, HttpUrl, field_validator
from typing import Optional

class Contact(BaseModel):
    name: str
    title: Optional[str] = None
    department: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    linkedin_url: Optional[HttpUrl] = None
    bio: Optional[str] = None

    # ensure empty strings become None
    @field_validator("*", mode="before")
    @classmethod
    def empty_to_none(cls, v):
        return v or None
