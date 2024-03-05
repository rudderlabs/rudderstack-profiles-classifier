from ..wht.rudderPB import RudderPB
from ..wht.mockPB import MockPB


def getPB():
    mock = False
    if mock:
        return MockPB()
    return RudderPB()
