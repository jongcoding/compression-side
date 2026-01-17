# /app/decision_attacker_binary.py
"""
decision_attacker_binary – 논문 레포 호환용 바이너리 어태커
- 원본 decision_attacker 인터페이스와 동일하게 동작
- loader 호환을 위해 여러 생성자 심볼을 export
"""
import sys, importlib

def _imp_local(modname: str):
    if "/app" not in sys.path:
        sys.path.insert(0, "/app")
    return importlib.import_module(modname)

_base = _imp_local("decision_attacker")

class decisionAttackerBinary(_base.decisionAttacker):
    def __init__(self, dbreacher, guesses, *args, ref_step=7, **kwargs):
        # *args로 fillerCharSet 등 3번째 인자까지 모두 상위에 전달
        super().__init__(dbreacher, guesses, *args, **kwargs)
        self._ref_step = max(1, int(ref_step))

    def setUp(self):
        # 힌트 전달(impl이 지원하면 사용)
        try:
            setattr(self.dbreacher, "ref_step_hint", self._ref_step)
        except Exception:
            pass
        return super().setUp()

# ─── loader 호환용 이름들 ───
decisionAttacker = decisionAttackerBinary
DecisionAttacker = decisionAttackerBinary
AttackerCtor     = decisionAttackerBinary

def create_attacker(*args, **kwargs):
    return decisionAttackerBinary(*args, **kwargs)

__all__ = [
    "decisionAttackerBinary",
    "decisionAttacker",
    "DecisionAttacker",
    "AttackerCtor",
    "create_attacker",
]
