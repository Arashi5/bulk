package bulk

type Error struct {
	Code string
	Msg  string
}

func NewError(code, msg string) Error {
	return Error{Code: code, Msg: msg}
}

func (s Error) Error() string {
	return s.Msg
}

const (
	OK string = "OK"

	Canceled string = "Canceled"

	Unknown string = "Unknown"

	InvalidArgument string = "InvalidArgument: %s"
)
