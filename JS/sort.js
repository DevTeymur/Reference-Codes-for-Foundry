function customSort(data, sortByKeys, sortOrder = 'asc') {
    // Get the length of the data array (assuming all sortByKeys have the same length).
    const length = data[sortByKeys[0]].length;
  
    // Iterate through the elements for sorting.
    for (let i = 0; i < length; i++) {
      for (let j = i + 1; j < length; j++) {
        let shouldSwap = false;
  
        // Compare elements based on the specified keys.
        for (const key of sortByKeys) {
          if (data[key][i] !== data[key][j]) {
            // Determine if a swap should occur based on sorting order.
            shouldSwap = (sortOrder === 'asc')
              ? data[key][i] > data[key][j]
              : data[key][i] < data[key][j];
            break; // Exit the loop if a decision is made.
          }
        }
  
        // If a swap should occur, swap all corresponding properties in the objects.
        if (shouldSwap) {
          for (const key of Object.keys(data)) {
            [data[key][i], data[key][j]] = [data[key][j], data[key][i]];
          }
        }
      }
    }
  }

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

// Either can be for one key or multiple
const sortByKeys = ['age', 'score'];
// asc or desc
const sortOrder = 'asc';

// Call the customSort function to sort the data.
customSort(data, sortByKeys, sortOrder);
console.log(data);

