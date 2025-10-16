from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Tuple

__all__ = ["BaseModel", "Field", "ValidationError"]


class ValidationError(ValueError):
    pass


@dataclass(slots=True)
class FieldInfo:
    default: Any = ...
    default_factory: Callable[[], Any] | None = None
    metadata: Dict[str, Any] | None = None


def Field(
    default: Any = ...,
    *,
    default_factory: Callable[[], Any] | None = None,
    **metadata: Any,
) -> FieldInfo:
    if default is not ... and default_factory is not None:
        raise ValueError("Cannot specify both default and default_factory")
    return FieldInfo(default=default, default_factory=default_factory, metadata=metadata or None)


class _ModelMeta(type):
    def __new__(mcls, name: str, bases: Tuple[type, ...], namespace: Dict[str, Any]):
        annotations: Dict[str, Any] = {}
        for base in reversed(bases):
            annotations.update(getattr(base, "__annotations__", {}))
        annotations.update(namespace.get("__annotations__", {}))

        fields: Dict[str, tuple[Any, FieldInfo]] = {}
        for attr, annotation in annotations.items():
            if attr.startswith("_") or attr == "__fields__":
                continue
            default = namespace.get(attr, ...)
            if isinstance(default, FieldInfo):
                info = default
                namespace.pop(attr, None)
            else:
                info = FieldInfo(default=default)
                namespace.pop(attr, None)
            fields[attr] = (annotation, info)

        namespace["__annotations__"] = annotations
        namespace["__fields__"] = fields
        return super().__new__(mcls, name, bases, namespace)


class BaseModel(metaclass=_ModelMeta):
    __fields__: Dict[str, tuple[Any, FieldInfo]]

    def __init__(self, **data: Any) -> None:
        for name, (annotation, info) in self.__fields__.items():
            if name in data:
                value = data[name]
            elif info.default is not ...:
                value = info.default
            elif info.default_factory is not None:
                value = info.default_factory()
            else:
                raise ValidationError(f"Missing value for field '{name}'")
            value = self._coerce(annotation, value)
            self._validate(name, info, value)
            object.__setattr__(self, name, value)

    # ------------------------------------------------------------------
    @staticmethod
    def _coerce(annotation: Any, value: Any) -> Any:
        if annotation in {int, float, str, bool} and not isinstance(value, annotation):
            if annotation is bool:
                if isinstance(value, str):
                    lowered = value.strip().lower()
                    if lowered in {"true", "1", "yes", "on"}:
                        return True
                    if lowered in {"false", "0", "no", "off"}:
                        return False
                return bool(value)
            try:
                return annotation(value)
            except Exception as exc:  # pragma: no cover - defensive
                raise ValidationError(f"Invalid value for {annotation}: {value!r}") from exc
        return value

    @staticmethod
    def _validate(name: str, info: FieldInfo, value: Any) -> None:
        if not info.metadata:
            return
        metadata = info.metadata
        if "gt" in metadata and not value > metadata["gt"]:
            raise ValidationError(f"{name} must be > {metadata['gt']}")
        if "ge" in metadata and not value >= metadata["ge"]:
            raise ValidationError(f"{name} must be >= {metadata['ge']}")

    # ------------------------------------------------------------------
    def model_dump(self) -> Dict[str, Any]:
        return {name: getattr(self, name) for name in self.__fields__}

    # ------------------------------------------------------------------
    def __setattr__(self, key: str, value: Any) -> None:  # pragma: no cover - immutability
        raise AttributeError("Instances of BaseModel are immutable in this lightweight stub")

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        fields = ", ".join(f"{name}={getattr(self, name)!r}" for name in self.__fields__)
        return f"{self.__class__.__name__}({fields})"
