namespace starrocks {
bool TUniqueId::operator<(const TUniqueId& rhs) const {
    return hi < rhs.hi || (hi == rhs.hi && lo < rhs.lo);
}
} // namespace