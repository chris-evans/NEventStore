using System;
using System.Collections;
using System.IO;
using System.Security.Cryptography;
using NEventStore.Logging;

namespace NEventStore.Serialization
{
    public class AES256Serializer : ISerialize
    {
        private const int KeyLength = 32; // bytes
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof(RijndaelSerializer));
        private readonly byte[] _encryptionKey;
        private readonly ISerialize _inner;

        public AES256Serializer(ISerialize inner, byte[] encryptionKey)
        {
            if (!KeyIsValid(encryptionKey, KeyLength))
            {
                throw new ArgumentException(Messages.InvalidKeyLength, "encryptionKey");
            }

            _encryptionKey = encryptionKey;
            _inner = inner;
        }

        public virtual void Serialize<T>(Stream output, T graph)
        {
            Logger.Verbose(Messages.SerializingGraph, typeof(T));

            using (var aes = new AesManaged())
            {
                aes.Key = _encryptionKey;
                aes.Mode = CipherMode.CBC;
                aes.GenerateIV();

                using (ICryptoTransform encryptor = aes.CreateEncryptor())
                using (var wrappedOutput = new IndisposableStream(output))
                using (var encryptionStream = new CryptoStream(wrappedOutput, encryptor, CryptoStreamMode.Write))
                {
                    wrappedOutput.Write(aes.IV, 0, aes.IV.Length);
                    _inner.Serialize(encryptionStream, graph);
                    encryptionStream.Flush();
                    encryptionStream.FlushFinalBlock();
                }
            }
        }

        public virtual T Deserialize<T>(Stream input)
        {
            Logger.Verbose(Messages.DeserializingStream, typeof(T));

            using (var aes = new AesManaged())
            {
                aes.Key = _encryptionKey;
                aes.IV = GetInitVectorFromStream(input, aes.IV.Length);
                aes.Mode = CipherMode.CBC;

                using (ICryptoTransform decryptor = aes.CreateDecryptor())
                using (var decryptedStream = new CryptoStream(input, decryptor, CryptoStreamMode.Read))
                    return _inner.Deserialize<T>(decryptedStream);
            }
        }

        private static bool KeyIsValid(ICollection key, int length)
        { return key != null && key.Count == length; }

        private static byte[] GetInitVectorFromStream(Stream encrypted, int initVectorSizeInBytes)
        {
            var buffer = new byte[initVectorSizeInBytes];
            encrypted.Read(buffer, 0, buffer.Length);
            return buffer;
        }
    }
}
