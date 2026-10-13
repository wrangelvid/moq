// The Rust interop preflight sends AUTH_ERROR for an expired token on moq-lite and moq-transport,
// then reads ours back.
import { SessionCode, SessionError } from "../../js/net/src/error.ts";
import type { Session as IetfSession } from "../../js/net/src/ietf/adapter.ts";
import { IetfAuthWire } from "../../js/net/src/ietf/auth.ts";
import { Version as IetfVersion } from "../../js/net/src/ietf/version.ts";
import { LiteAuthWire } from "../../js/net/src/lite/auth.ts";
import { Version as LiteVersion } from "../../js/net/src/lite/version.ts";
import { Stream, type StreamVersion } from "../../js/net/src/stream.ts";

const input = JSON.parse(process.argv[2]) as { lite: number[]; ietf: number[] };

async function echo(wire: LiteAuthWire | IetfAuthWire, version: StreamVersion, bytes: number[]): Promise<number[]> {
	const encoded: Uint8Array[] = [];
	const stream = new Stream({
		readable: new ReadableStream<Uint8Array>({
			start(controller) {
				controller.enqueue(Uint8Array.from(bytes));
				controller.close();
			},
		}),
		writable: new WritableStream<Uint8Array>({
			write(chunk) {
				encoded.push(new Uint8Array(chunk));
			},
		}),
		version,
	});
	const reply = await wire.read(stream);
	if (!reply || !("refused" in reply) || !(reply.refused instanceof SessionError)) {
		throw new Error("Rust refusal missing");
	}
	if (reply.refused.code !== SessionCode.Expired) throw new Error(`Rust expiry read as ${reply.refused.code}`);
	await wire.refuse(stream, SessionCode.Expired, "expired");
	stream.writer.close();
	await stream.writer.closed;
	return [...Buffer.concat(encoded)];
}

const lite = await echo(new LiteAuthWire({} as WebTransport, LiteVersion.DRAFT_07), LiteVersion.DRAFT_07, input.lite);
const ietf = await echo(
	new IetfAuthWire({ version: IetfVersion.DRAFT_17 } as IetfSession),
	IetfVersion.DRAFT_17,
	input.ietf,
);
process.stdout.write(JSON.stringify({ lite, ietf }));
