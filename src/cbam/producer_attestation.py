class ProducerAttestation:
    def sign(self, producer_id, declaration):
        return {"producer":producer_id,"attested":True}
