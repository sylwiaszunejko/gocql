package mod

var All = []Mod{CustomType, Reference, CustomTypeRef}

// Mod - value modifiers.
type Mod func(vals ...interface{}) []interface{}

type Values []interface{}

func (v Values) AddVariants(mods ...Mod) Values {
	out := append(make([]interface{}, 0), v...)
	for _, mod := range mods {
		out = append(out, mod(v...)...)
	}
	return out
}
