package uuid

func makeTestUUID(versionNumber byte, variant byte) UUID {
	return [16]byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, versionNumber, 0x0, variant, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
}
