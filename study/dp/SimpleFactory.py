import inspect
import os


class Operation:
    def GetResult(self):
        pass


class OperationAdd(Operation):
    def GetResult(self):
        return self.op1 + self.op2


class OperationSub(Operation):
    def GetResult(self):
        return self.op1 - self.op2


class OperationMul(Operation):
    def GetResult(self):
        return self.op1 * self.op2


class OperationDiv(Operation):
    def GetResult(self):
        try:
            result = self.op1 / self.op2
            return result
        except:
            print("error:divided by zero.")
            return 0


class OperationFac(Operation):
    def GetResult(self):
        i = 1
        sum = 1
        while i <= self.op1:
            sum *= i
            i += 1
        return sum


class OperationUndef(Operation):
    def GetResult(self):
        print("Undefine operation.")
        return 0


class OperationFactory:
    operation = {}
    operation["+"] = OperationAdd();
    operation["-"] = OperationSub();
    operation["*"] = OperationMul();
    operation["/"] = OperationDiv();
    operation["!"] = OperationFac();

    def createOperation(self, ch):
        if ch in self.operation:
            op = self.operation[ch]
        else:
            op = OperationUndef()
        return op


if __name__ == "__main__":
    op = input("operator: ")
    opa = input("a: ")
    opb = input("b: ")
    factory = OperationFactory()
    cal = factory.createOperation(op)
    cal.op1 = opa
    cal.op2 = opb
    print(cal.GetResult())
