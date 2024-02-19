from src.wht.rudderPB import RudderPB
from src.wht.mockPB import MockPB


def getPB():
    mock = False
    if mock:
        return MockPB()
    return RudderPB()
