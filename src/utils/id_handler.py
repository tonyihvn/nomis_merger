def generate_unique_ids(existing_ids, new_ids):
    unique_ids = set(existing_ids)
    id_mapping = {}
    
    for new_id in new_ids:
        if new_id not in unique_ids:
            id_mapping[new_id] = new_id
            unique_ids.add(new_id)
        else:
            # Generate a new unique ID
            counter = 1
            while f"{new_id}_{counter}" in unique_ids:
                counter += 1
            new_unique_id = f"{new_id}_{counter}"
            id_mapping[new_id] = new_unique_id
            unique_ids.add(new_unique_id)
    
    return id_mapping

def remove_specific_records(records, ids_to_remove):
    return [record for record in records if record['id'] not in ids_to_remove]

def maintain_relationships(original_records, new_records, id_mapping):
    for record in new_records:
        if record['related_id'] in id_mapping:
            record['related_id'] = id_mapping[record['related_id']]
    return new_records