import time
import json
import ijson
import chardet

from database.funcs import detect_encoding


start_time = time.time()
end_time = None


input_file = './dataset/dblp.v12.json'
output_file = './dataset/unique_venue_types.json'

unique_venue_types = set()

encoding = detect_encoding(input_file)

print(f'Detected encoding: {encoding}')


with open(input_file, 'r', encoding=encoding) as f:
    for i, item in enumerate(ijson.items(f, "item")):
        venue_type = item.get('venue', {}).get('type')

        if venue_type:
            unique_venue_types.add(venue_type)

unique_venue_types_list = list(unique_venue_types)
print(f'Unique venue types: {len(unique_venue_types_list)}')


with open(output_file, 'w', encoding=encoding) as out_file:
    json.dump(unique_venue_types_list, out_file, indent=4)


end_time = time.time()
print(f'Execution time: {end_time - start_time} seconds')