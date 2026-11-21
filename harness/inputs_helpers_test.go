package harness

import "github.com/Syntheresis/melta/model"

// testInputs returns stored Session inputs distinguished by their
// instructions choice.
func testInputs(name string) SessionInputs {
	return SessionInputs{Instructions: &name}
}

// modelChoice returns an owned stored model choice.
func modelChoice(ref model.ModelRef) *model.ModelRef {
	return &ref
}

// modelChoiceValue reads a stored model choice; absence is the zero value.
func modelChoiceValue(choice *model.ModelRef) model.ModelRef {
	if choice == nil {
		return model.ModelRef{}
	}
	return *choice
}
