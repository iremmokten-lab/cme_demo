
import hashlib, json

def hash_json(data):
    return hashlib.sha256(
        json.dumps(data, sort_keys=True).encode()
    ).hexdigest()

def verify_replay(snapshot_input, snapshot_result, new_result):
    input_hash = hash_json(snapshot_input)
    result_hash_original = hash_json(snapshot_result)
    result_hash_new = hash_json(new_result)

    return {
        "input_hash": input_hash,
        "original_result_hash": result_hash_original,
        "replayed_result_hash": result_hash_new,
        "match": result_hash_original == result_hash_new
    }
