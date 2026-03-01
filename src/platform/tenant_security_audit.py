class TenantAudit:
    def record(self,tenant,action):
        return {"tenant":tenant,"action":action}
