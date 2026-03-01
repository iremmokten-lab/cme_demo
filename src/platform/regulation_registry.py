class RegulationRegistry:
    def __init__(self):
        self.specs={}
    def register(self,name,version):
        self.specs[name]=version
