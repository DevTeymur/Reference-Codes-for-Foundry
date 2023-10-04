// Function that creates an empty object with the same keys as the input object
function createEmptyObjectWithKeys(obj) {
    const emptyObject = {};
  
    // Loop through the keys of the input object
    for (const key in obj) {
      // Check if the key is a direct property of the object (not inherited)
      if (obj.hasOwnProperty(key)) {
        // Initialize each key in the empty object with an empty array
        emptyObject[key] = [];
      }
    }
  
    return emptyObject;
}

// Function that filters data based on a specified key and filter criteria
function customFilter(data, key, filter_by) {
    // Create an empty object with the same keys as the input 'data' object
    final_data = createEmptyObjectWithKeys(data);
    
    // Get first key of the data in order to measure the length of the 'rows'
    const first_key_of_data = Object.keys(data)[0]
    // Get the length of the 'wellname' array in the 'data' object
    data_length = data[first_key_of_data].length;
    
    // Get an array of all keys in the 'data' object
    all_keys = Object.keys(data);
  
    // Loop through the data
    for (i = 0; i < data_length; i++) {
        // Check if the value at the specified key matches any value in the 'filter_by' array
        if (filter_by.includes(data[key][i])) {
            // If it does, copy all values for this index across all keys to the 'final_data' object
            for (j = 0; j < all_keys.length; j++) {
                final_data[all_keys[j]].push(data[all_keys[j]][i]);
            }
        }
    }
    return final_data;
}


// Example call

const data = {
    "name": [
        "John", "Alice", "Bob", "Eve"
    ],
    "age": [
        30, 25, 30, 21
    ],
    "score": [
        85, 92, 78, 92
    ]
};


filtered_data = customFilter(data, 'name', 'John')
console.log(filtered_data)
