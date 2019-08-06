using System;
using System.IO;

namespace NEventStore.Persistence.AzureBlob
{
	/// <summary>
	/// Definition of the header
	/// </summary>
	internal class HeaderDefinitionMetadata
	{
		public const int RawSize = 8;

		/// <summary>
		/// Get the start location of the header in bytes
		/// </summary>
		public int HeaderStartLocationOffsetBytes
		{ get; set; }

		/// <summary>
		/// Get the size of the header in bytes
		/// </summary>
		public int HeaderSizeInBytes
		{ get; set; }

		/// <summary>
		/// Get the raw data.
		/// </summary>
		/// <returns></returns>
		public byte[] GetRaw()
		{
			using (var ms = new MemoryStream())
			{
				WriteToMs(ms, BitConverter.GetBytes(HeaderStartLocationOffsetBytes));
				WriteToMs(ms, BitConverter.GetBytes(HeaderSizeInBytes));
				return ms.ToArray();
			}
		}

		/// <summary>
		/// Clones the header definition
		/// </summary>
		/// <returns></returns>
		public HeaderDefinitionMetadata Clone()
		{
			return new HeaderDefinitionMetadata()
			{
				HeaderStartLocationOffsetBytes = this.HeaderStartLocationOffsetBytes,
				HeaderSizeInBytes = this.HeaderSizeInBytes
			};
		}

		/// <summary>
		/// Create a header definition from raw data
		/// </summary>
		/// <param name="raw"></param>
		/// <returns></returns>
		public static HeaderDefinitionMetadata FromRaw(byte[] raw)
		{
			return new HeaderDefinitionMetadata()
			{
				HeaderStartLocationOffsetBytes = BitConverter.ToInt32(raw, 0),
				HeaderSizeInBytes = BitConverter.ToInt32(raw, 4),
			};
		}

		private static void WriteToMs(MemoryStream ms, byte[] data)
		{ ms.Write(data, 0, data.Length); }
	}
}
