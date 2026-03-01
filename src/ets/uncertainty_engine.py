class UncertaintyEngine:
    def calculate(self, measurements):
        if not measurements: return 0
        avg=sum(measurements)/len(measurements)
        variance=sum((x-avg)**2 for x in measurements)/len(measurements)
        return variance**0.5
