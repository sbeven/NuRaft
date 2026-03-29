/************************************************************************
 * Simple log record for nuraft logging example
 * - contains an operation enum (PUT/DEL) and key/value strings
 * - provides serialize() and static deserialize(buffer&)
 ************************************************************************/

#pragma once

#include "nuraft.hxx"

#include <string>
#include <sstream>

using namespace nuraft;

struct nl_log {
	enum op_type : int32_t {
		PUT = 0,
		DEL = 1
	};

	op_type op{};
	std::string key;
	std::string value; // empty for DEL
	uint64_t csn = 0;

	nl_log() = default;
	nl_log(op_type o, std::string k, std::string v = {}, uint64_t c = 0)
		: op(o), key(std::move(k)), value(std::move(v)), csn(c) {}

	// Serialize to a NuRaft buffer (length-prefixed strings).
	ptr<buffer> serialize() const {
		// Use buffer_serializer helper that writes length-prefixed strings.
		// Reserve approx size: 4 bytes for op + string lengths + csn + overhead.
		size_t approx = sizeof(int32_t) + key.size() + value.size() + sizeof(uint64_t) + 16;
		ptr<buffer> ret = buffer::alloc(static_cast<int>(approx));
		buffer_serializer bs(ret);
		bs.put_i32(static_cast<int32_t>(op));
		bs.put_str(key);
		bs.put_str(value);
		bs.put_u64(csn);
		return ret;
	}

	// Deserialize from an existing buffer.
	static nl_log deserialize(buffer& buf) {
		buffer_serializer bs(buf);
		nl_log out;
		out.op = static_cast<op_type>( bs.get_i32() );
		out.key = bs.get_str();
		out.value = bs.get_str();
		out.csn = bs.get_u64();
		return out;
	}

	// Human-readable string for debugging.
	std::string to_string() const {
		std::ostringstream os;
		os << "nl_log{";
		switch (op) {
		case PUT: os << "op=PUT"; break;
		case DEL: os << "op=DEL"; break;
		default:  os << "op=UNKNOWN(" << static_cast<int>(op) << ")"; break;
		}
		os << ", key='" << key << "'";
		if (op == PUT) os << ", value='" << value << "'";
		os << ", csn=" << csn;
		os << "}";
		return os.str();
	}
};

