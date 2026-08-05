"""Implementação de `JobQueue` (Marco 2) sobre `arq`."""

from adapters.queue.arq_queue import ArqJobQueue

__all__ = ["ArqJobQueue"]
