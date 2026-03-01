class CalibrationRegistry:
    def register(self, device_id, certificate):
        return {"device":device_id,"certificate":certificate}
