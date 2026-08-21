import { hmac } from "@noble/hashes/hmac.js";
import { md5 } from "@noble/hashes/legacy.js";
import { sha256 } from "@noble/hashes/sha2.js";
import { bytesToHex, concatBytes, utf8ToBytes } from "@noble/hashes/utils.js";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const md5Hex = (bytes: Uint8Array): string => bytesToHex(md5(bytes));

export const hmacSha256Hex = (secret: string, message: string): string =>
  bytesToHex(hmac(sha256, utf8ToBytes(secret), utf8ToBytes(message)));

export const timingSafeEqual = (left: string, right: string): boolean => {
  const leftBytes = utf8ToBytes(left);
  const rightBytes = utf8ToBytes(right);
  let difference = leftBytes.length ^ rightBytes.length;
  const length = Math.max(leftBytes.length, rightBytes.length);
  for (let index = 0; index < length; index += 1) {
    difference |= (leftBytes[index % leftBytes.length] ?? 0) ^ (rightBytes[index % rightBytes.length] ?? 0);
  }
  return difference === 0;
};

export const channelAuthorization = (
  appKey: string,
  appSecret: string,
  socketId: string,
  channel: string,
  channelData?: string,
): string => {
  const message =
    channelData === undefined
      ? `${socketId}:${channel}`
      : `${socketId}:${channel}:${channelData}`;
  return `${appKey}:${hmacSha256Hex(appSecret, message)}`;
};

export const userAuthentication = (
  appKey: string,
  appSecret: string,
  socketId: string,
  userData: string,
): string => `${appKey}:${hmacSha256Hex(appSecret, `${socketId}::user::${userData}`)}`;

export const verifyChannelAuthorization = (
  authorization: string,
  appKey: string,
  appSecret: string,
  socketId: string,
  channel: string,
  channelData?: string,
): boolean =>
  timingSafeEqual(
    authorization,
    channelAuthorization(appKey, appSecret, socketId, channel, channelData),
  );

export const verifyUserAuthentication = (
  authorization: string,
  appKey: string,
  appSecret: string,
  socketId: string,
  userData: string,
): boolean =>
  timingSafeEqual(authorization, userAuthentication(appKey, appSecret, socketId, userData));

export const encryptedChannelSharedSecret = Effect.fn("Pusher.encryptedChannelSharedSecret")(
  (channel: string, encryptionMasterKey: Uint8Array) =>
    Schema.encodeEffect(Schema.Uint8ArrayFromBase64)(
      sha256(concatBytes(utf8ToBytes(channel), encryptionMasterKey)),
    ),
);

export const parseBase64 = Schema.decodeEffect(Schema.Uint8ArrayFromBase64);
