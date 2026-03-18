// Updated merge_ht method in JoinHashTable
void JoinHashTable::merge_ht(...) {
    // existing code...

    // Defensive check for other_key_columns
    for (size_t i = 0; i < other_key_columns.size(); ++i) {
        if (other_key_columns[i]->size() > 0) {
            key_columns.append(*other_key_columns[i], 1, other_key_columns[i]->size() - 1);
        }
    }
    // Keep nullable upgrade logic...
}