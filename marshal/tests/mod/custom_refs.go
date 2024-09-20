package mod

var CustomTypeRef Mod = func(vals ...interface{}) []interface{} {
	return Reference(CustomType(vals...)...)
}
