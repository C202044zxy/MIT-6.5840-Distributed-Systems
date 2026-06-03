package rpc

type Err string

const (
	// Err's returned by server and Clerk
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"
)

type Tversion uint64

type PutArgs struct {
	Key      string
	Value    string
	Version  Tversion
	ClientId int64
	SeqNum   int64
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientId int64
	SeqNum   int64
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}

// --- Transaction comparison ---
type CmpTarget int

const (
	CmpVersion CmpTarget = iota
	CmpValue
)

type CmpOp int

const (
	CmpEqual CmpOp = iota
	CmpNotEqual
	CmpLess
	CmpGreater
)

type Compare struct {
	Key     string
	Target  CmpTarget // compare the key's Version or its Value
	Op      CmpOp
	Version Tversion // expected, when Target==CmpVersion
	Value   string   // expected, when Target==CmpValue
}

// --- Transaction op ---
type TxnOpType int

const (
	OpGet TxnOpType = iota
	OpPut
)

type TxnOp struct {
	Type  TxnOpType
	Key   string
	Value string // for OpPut
}

// --- RPC payloads ---
type TxnArgs struct {
	Conds    []Compare
	ThenOps  []TxnOp
	ElseOps  []TxnOp
	ClientId int64
	SeqNum   int64
}

type OpResult struct { // one per executed TxnOp, in order
	Value   string   // OpGet
	Version Tversion // OpGet (0 ⇒ key absent), or new version after OpPut
	Exists  bool     // OpGet: whether key existed
	Err     Err      // per-op error (e.g. ErrNoKey on a Get)
}

type TxnReply struct {
	Succeeded bool // true ⇒ Conds all held, ThenOps ran; false ⇒ ElseOps ran
	Results   []OpResult
	Err       Err // OK / ErrWrongLeader
}
