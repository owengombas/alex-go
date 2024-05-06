package shared

import (
	"math"
	"unsafe"
)

const KeySize = int(unsafe.Sizeof(KeyType(0)))
const PayloadSize = int(unsafe.Sizeof(PayloadType(0)))
const BlockSize = KeySize + PayloadSize
const MaxKey = math.MaxInt
const MinKey = math.MinInt

// KMaxDensity Variables related to resizing (expansions and contractions)
// Density after contracting, also determines the expansion threshold
const KMaxDensity = 0.8

// KInitialDensity Density of data nodes after bulk loading
const KInitialDensity = 0.7

// KMinDensity Density after expanding, also determines the contraction threshold
const KMinDensity = 0.6

// KExpSearchIterationsWeight Intra-node cost weights
const KExpSearchIterationsWeight = 20.0

// KShiftsWeight Intra-node cost weights
const KShiftsWeight = 0.5

// KNodeLookupsWeight TraverseToLeaf cost weights
const KNodeLookupsWeight = 20.0

// KDefaultMaxDataNodeBytes By default, maximum data node size is 16MB
const KDefaultMaxDataNodeBytes = 1 << 24

// MaxSlots The maximum number of slots in a data node
const MaxSlots = KDefaultMaxDataNodeBytes / BlockSize

// KAppendMostlyThreshold Node is considered append-mostly if the fraction of inserts that are out of
// bounds is above this threshold
// Append-mostly nodes will expand in a manner that anticipates further
// appends
const KAppendMostlyThreshold = 0.9

// KEndSentinel Placed at the end of the key/data slots if there are gaps after the max key
const KEndSentinel = MaxKey

// KModelSizeWeight TraverseToLeaf cost weights
const KModelSizeWeight = 5e-7

// KMinOutOfDomainKeys At least this many keys must be outside the domain before a domain expansion is triggered.
const KMinOutOfDomainKeys = 5

// KMaxOutOfDomainKeys After this many keys are outside the domain, a domain expansion must be  triggered.
const KMaxOutOfDomainKeys = 1000

// KOutOfDomainToleranceFactor When the number of max out-of-domain (OOD) keys is between the min and
// max, expand the domain if the number of OOD keys is greater than the
// expected number of OOD due to randomness by greater than the tolereance
// factor.
const KOutOfDomainToleranceFactor = 2

// CatastropheCheckFrequency The frequency of catastrophic checks while inserting keys to a data node.
const CatastropheCheckFrequency = 64

// NumKeysDataNodeRetrainThreshold The number of keys that must be inserted before the model on a data node is retrained.
const NumKeysDataNodeRetrainThreshold = 50

// FanoutSelectionMethod Fanout selection method used during bulk loading: 0 means use bottom-up fanout tree, 1 means top-down
const FanoutSelectionMethod int = 0

const (
	// 0 means always split node in 2
	AlwaysSplitNodeInTwo = iota
	// 1 means decide between no splitting or splitting in 2
	DecideBetweenNoSplittingOrSplittingInTwo = iota
	// 2 means use a full fanout tree to decide the splitting strategy
	UseFullFanoutTree = iota
)

// Policy when a data node experiences significant cost deviation.
const SplittingPolicyMethod int = DecideBetweenNoSplittingOrSplittingInTwo

// Splitting upwards means that a split can propagate all the way up to the
// root, like a B+ tree
// Splitting upwards can result in a better RMI, but has much more overhead
// than splitting sideways
const AllowSplittingUpwards bool = false
