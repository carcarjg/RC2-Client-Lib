using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Newtonsoft.Json;
using WebSocketSharp;
using SIPSorcery.Net;
using SIPSorcery.Media;
using SIPSorceryMedia.Abstractions;
using NAudio.Wave;

namespace RC2ClientLibrary
{
	/// <summary>
	/// Client library for connecting to RC2Server
	/// Handles WebSocket messaging, WebRTC audio streaming, and stores server data
	/// </summary>
	public class RC2Client : IDisposable
	{
		#region Private Fields

		private WebSocket ws;

		private RTCPeerConnection pc;

		private MediaStreamTrack audioTrack;

		private AudioEncoder txEncoder;

		private AudioEncoder rxEncoder;

		// Audio devices
		private WaveInEvent microphoneInput;

		private WaveOutEvent speakerOutput;

		private BufferedWaveProvider audioBuffer;

		// Connection settings
		private string serverUrl;

		private int microphoneDeviceNumber = -1;

		private int speakerDeviceNumber = -1;

		// Audio format
		private AudioFormat negotiatedFormat;

		private const string CODEC = "G722";

		#endregion Private Fields

		#region Public Properties - Stored Server Data

		/// <summary>
		/// Current radio status received from server
		/// </summary>
		public RadioStatus CurrentStatus { get; private set; }

		/// <summary>
		/// Last acknowledgment command received
		/// </summary>
		public string LastAck { get; private set; }

		/// <summary>
		/// Last negative acknowledgment command received
		/// </summary>
		public string LastNack { get; private set; }

		/// <summary>
		/// Indicates if WebSocket is connected
		/// </summary>
		public bool IsWebSocketConnected => ws?.ReadyState == WebSocketState.Open;

		/// <summary>
		/// Indicates if WebRTC is connected
		/// </summary>
		public bool IsWebRtcConnected => pc?.connectionState == RTCPeerConnectionState.connected;

		/// <summary>
		/// Indicates if audio is currently being transmitted
		/// </summary>
		public bool IsTransmitting { get; private set; }

		#endregion Public Properties - Stored Server Data

		#region Events

		/// <summary>
		/// Fired when radio status is received from server
		/// </summary>
		public event EventHandler<RadioStatus> StatusReceived;

		/// <summary>
		/// Fired when WebSocket connects
		/// </summary>
		public event EventHandler WebSocketConnected;

		/// <summary>
		/// Fired when WebSocket disconnects
		/// </summary>
		public event EventHandler<string> WebSocketDisconnected;

		/// <summary>
		/// Fired when WebRTC connects
		/// </summary>
		public event EventHandler WebRtcConnected;

		/// <summary>
		/// Fired when WebRTC disconnects
		/// </summary>
		public event EventHandler WebRtcDisconnected;

		/// <summary>
		/// Fired when an error occurs
		/// </summary>
		public event EventHandler<string> ErrorOccurred;

		/// <summary>
		/// Fired when ACK is received from server
		/// </summary>
		public event EventHandler<string> AckReceived;

		/// <summary>
		/// Fired when NACK is received from server
		/// </summary>
		public event EventHandler<string> NackReceived;

		#endregion Events

		#region Constructor

		/// <summary>
		/// Create a new RC2Client instance
		/// </summary>
		/// <param name="serverAddress">Server address (e.g., "192.168.1.100")</param>
		/// <param name="serverPort">Server port (default: 8080)</param>
		public RC2Client(string serverAddress, int serverPort = 8080)
		{
			serverUrl = $"ws://{serverAddress}:{serverPort}";
			CurrentStatus = new RadioStatus();

			// Initialize encoders
			txEncoder = new AudioEncoder();
			rxEncoder = new AudioEncoder();
		}

		#endregion Constructor

		#region Audio Device Configuration

		/// <summary>
		/// Set the microphone device to use for transmission
		/// </summary>
		/// <param name="deviceNumber">Device number (-1 for default)</param>
		public void SetMicrophoneDevice(int deviceNumber)
		{
			microphoneDeviceNumber = deviceNumber;
		}

		/// <summary>
		/// Set the speaker device to use for reception
		/// </summary>
		/// <param name="deviceNumber">Device number (-1 for default)</param>
		public void SetSpeakerDevice(int deviceNumber)
		{
			speakerDeviceNumber = deviceNumber;
		}

		/// <summary>
		/// Get list of available input (microphone) devices
		/// </summary>
		public static List<AudioDeviceInfo> GetInputDevices()
		{
			var devices = new List<AudioDeviceInfo>();
			for (int i = 0; i < WaveInEvent.DeviceCount; i++)
			{
				var caps = WaveInEvent.GetCapabilities(i);
				devices.Add(new AudioDeviceInfo
				{
					DeviceNumber = i,
					Name = caps.ProductName,
					Channels = caps.Channels
				});
			}
			return devices;
		}

		/// <summary>
		/// Get list of available output (speaker) devices
		/// </summary>
		public static List<AudioDeviceInfo> GetOutputDevices()
		{
			var devices = new List<AudioDeviceInfo>();
			for (int i = 0; i < WaveOut.DeviceCount; i++)
			{
				var caps = WaveOut.GetCapabilities(i);
				devices.Add(new AudioDeviceInfo
				{
					DeviceNumber = i,
					Name = caps.ProductName,
					Channels = caps.Channels
				});
			}
			return devices;
		}

		#endregion Audio Device Configuration

		#region Connection Methods

		/// <summary>
		/// Connect to the RC2Server
		/// </summary>
		public async Task<bool> ConnectAsync()
		{
			try
			{
				// Step 1: Connect main WebSocket for control messages
				await ConnectWebSocketAsync();

				// Step 2: Query radio status (like JS client does immediately)
				QueryStatus();

				// Step 3: Wait a moment for status response
				await Task.Delay(500);

				// Step 4: Connect WebRTC for audio (like JS client does after status)
				await ConnectWebRtcAsync();

				return true;
			}
			catch (Exception ex)
			{
				ErrorOccurred?.Invoke(this, $"Connection failed: {ex.Message}");
				return false;
			}
		}

		/// <summary>
		/// Connect WebSocket for control messages
		/// </summary>
		private Task ConnectWebSocketAsync()
		{
			var tcs = new TaskCompletionSource<bool>();

			ws = new WebSocket($"{serverUrl}/");

			ws.OnOpen += (sender, e) =>
			{
				WebSocketConnected?.Invoke(this, EventArgs.Empty);
				tcs.TrySetResult(true);
			};

			ws.OnMessage += OnWebSocketMessage;

			ws.OnError += (sender, e) =>
			{
				ErrorOccurred?.Invoke(this, $"WebSocket error: {e.Message}");
				tcs.TrySetException(new Exception(e.Message));
			};

			ws.OnClose += (sender, e) =>
			{
				WebSocketDisconnected?.Invoke(this, e.Reason);
			};

			ws.Connect();

			return tcs.Task;
		}

		/// <summary>
		/// Connect WebRTC for audio streaming
		/// </summary>
		private async Task ConnectWebRtcAsync()
		{
			try
			{
				// Create peer connection configuration (match JavaScript client)
				var config = new RTCConfiguration
				{
					// No STUN/TURN servers by default to match JS client
				};
				pc = new RTCPeerConnection(config);

				// Get only G.722 format from supported formats
				var g722Format = rxEncoder.SupportedFormats.Find(f => f.FormatName == CODEC);

				// Setup audio track with only G.722 (SendRecv to match JS client)
				audioTrack = new MediaStreamTrack(
					new List<AudioFormat> { g722Format },
					MediaStreamStatusEnum.SendRecv
				);
				pc.addTrack(audioTrack);

				// Handle format negotiation
				pc.OnAudioFormatsNegotiated += (formats) =>
				{
					// Verify only G.722 was negotiated
					var g722 = formats.Find(f => f.FormatName == CODEC);

					OnAudioFormatsNegotiated(new List<AudioFormat> { g722 });
				};

				// Handle connection state changes
				pc.onconnectionstatechange += OnRtcConnectionStateChange;

				// Handle incoming audio
				pc.OnRtpPacketReceived += OnRtpPacketReceived;

				// Connect to WebRTC signaling WebSocket (separate from main WS)
				// IMPORTANT: The SERVER sends the offer, CLIENT responds with answer
				var rtcWs = new WebSocket($"{serverUrl}/rtc");
				var answerSent = new TaskCompletionSource<bool>();
				var timeout = Task.Delay(10000); // 10 second timeout

				rtcWs.OnOpen += (sender, e) =>
				{
					// Just wait for server offer - don't send anything yet
				};

				rtcWs.OnMessage += (sender, e) =>
				{
					try
					{
						var message = JsonConvert.DeserializeObject<Dictionary<string, object>>(e.Data);

						// Check if this is an ICE candidate
						if (message.ContainsKey("candidate"))
						{
							// Handle ICE candidate from server
							var candidateInit = JsonConvert.DeserializeObject<RTCIceCandidateInit>(e.Data);
							pc.addIceCandidate(candidateInit);
						}

						// Check if this is an SDP offer from server
						else if (message.ContainsKey("sdp"))
						{
							var sdpMessage = JsonConvert.DeserializeObject<Dictionary<string, string>>(e.Data);

							// Server sends offer, we create answer
							var offer = new RTCSessionDescriptionInit
							{
								type = RTCSdpType.offer,
								sdp = sdpMessage["sdp"]
							};

							// Set remote description (the offer from server)
							var setRemoteResult = pc.setRemoteDescription(offer);
							if (setRemoteResult != SetDescriptionResultEnum.OK)
							{
								throw new Exception($"Failed to set remote description: {setRemoteResult}");
							}

							// Create answer
							var answer = pc.createAnswer(null);

							// Set local description (our answer)
							var setLocalResult = pc.setLocalDescription(answer);

							// Send answer back to server
							var answerJson = new
							{
								type = "answer",
								sdp = answer.sdp
							};
							rtcWs.Send(JsonConvert.SerializeObject(answerJson));

							answerSent.TrySetResult(true);
						}
					}
					catch (Exception ex)
					{
						ErrorOccurred?.Invoke(this, $"WebRTC signaling error: {ex.Message}");
						answerSent.TrySetException(ex);
					}
				};

				// Handle ICE candidates from local peer
				pc.onicecandidate += (candidate) =>
				{
					if (candidate != null && rtcWs.ReadyState == WebSocketState.Open)
					{
						var candidateJson = new
						{
							candidate = candidate.candidate,
							sdpMLineIndex = candidate.sdpMLineIndex,
							sdpMid = candidate.sdpMid
						};
						rtcWs.Send(JsonConvert.SerializeObject(candidateJson));
					}
				};

				rtcWs.OnError += (sender, e) =>
				{
					ErrorOccurred?.Invoke(this, $"RTC WebSocket error: {e.Message}");
					if (!answerSent.Task.IsCompleted)
					{
						answerSent.TrySetException(new Exception($"RTC WebSocket error: {e.Message}"));
					}
				};

				rtcWs.Connect();

				// Wait for answer to be sent with timeout
				var completedTask = await Task.WhenAny(answerSent.Task, timeout);
				if (completedTask == timeout)
				{
					rtcWs.Close();
					throw new TimeoutException("WebRTC signaling timeout after 10 seconds");
				}

				await answerSent.Task; // Re-throw any exceptions
			}
			catch (Exception ex)
			{
				ErrorOccurred?.Invoke(this, $"WebRTC connection failed: {ex.Message}");
				throw;
			}
		}

		/// <summary>
		/// Disconnect from server
		/// </summary>
		public void Disconnect()
		{
			StopAudio();

			if (pc != null)
			{
				pc.Close("Client disconnect");
				pc = null;
			}

			if (ws != null)
			{
				ws.Close();
				ws = null;
			}
		}

		#endregion Connection Methods

		#region Audio Control Methods

		/// <summary>
		/// Start audio streaming (both transmit and receive)
		/// </summary>
		public void StartAudio()
		{
			// Setup speaker output
			SetupSpeakerOutput();

			// Setup microphone input
			SetupMicrophoneInput();
		}

		/// <summary>
		/// Stop audio streaming
		/// </summary>
		public void StopAudio()
		{
			if (microphoneInput != null)
			{
				microphoneInput.StopRecording();
				microphoneInput.Dispose();
				microphoneInput = null;
			}

			if (speakerOutput != null)
			{
				speakerOutput.Stop();
				speakerOutput.Dispose();
				speakerOutput = null;
			}

			if (audioBuffer != null)
			{
				audioBuffer.ClearBuffer();
				audioBuffer = null;
			}
		}

		/// <summary>
		/// Setup microphone input for transmission
		/// </summary>
		private void SetupMicrophoneInput()
		{
			try
			{
				microphoneInput = new WaveInEvent
				{
					DeviceNumber = microphoneDeviceNumber,
					WaveFormat = new WaveFormat(negotiatedFormat.ClockRate, 16, 1),
					BufferMilliseconds = 20
				};

				microphoneInput.DataAvailable += OnMicrophoneDataAvailable;
				microphoneInput.StartRecording();
			}
			catch (Exception ex)
			{
				ErrorOccurred?.Invoke(this, $"Failed to setup microphone: {ex.Message}");
			}
		}

		/// <summary>
		/// Setup speaker output for reception
		/// </summary>
		private void SetupSpeakerOutput()
		{
			try
			{
				var waveFormat = new WaveFormat(negotiatedFormat.ClockRate, 16, 1);
				audioBuffer = new BufferedWaveProvider(waveFormat)
				{
					BufferLength = waveFormat.AverageBytesPerSecond * 2,
					DiscardOnBufferOverflow = true
				};

				speakerOutput = new WaveOutEvent
				{
					DeviceNumber = speakerDeviceNumber,
					DesiredLatency = 100
				};

				speakerOutput.Init(audioBuffer);
				speakerOutput.Play();
			}
			catch (Exception ex)
			{
				ErrorOccurred?.Invoke(this, $"Failed to setup speaker: {ex.Message}");
			}
		}

		#endregion Audio Control Methods

		#region Radio Control Commands

		/// <summary>
		/// Query radio status from server
		/// </summary>
		public void QueryStatus()
		{
			SendCommand(new { radio = new { command = "query" } });
		}

		/// <summary>
		/// Start transmitting
		/// </summary>
		public void StartTransmit()
		{
			SendCommand(new { radio = new { command = "startTx" } });
			IsTransmitting = true;
		}

		/// <summary>
		/// Stop transmitting
		/// </summary>
		public void StopTransmit()
		{
			SendCommand(new { radio = new { command = "stopTx" } });
			IsTransmitting = false;
		}

		/// <summary>
		/// Change channel up
		/// </summary>
		public void ChannelUp()
		{
			SendCommand(new { radio = new { command = "chanUp" } });
		}

		/// <summary>
		/// Change channel down
		/// </summary>
		public void ChannelDown()
		{
			SendCommand(new { radio = new { command = "chanDn" } });
		}

		/// <summary>
		/// Press a button on the radio
		/// </summary>
		public void PressButton(SoftkeyName button)
		{
			SendCommand(new { radio = new { command = "buttonPress", options = button.ToString() } });
		}

		/// <summary>
		/// Release a button on the radio
		/// </summary>
		public void ReleaseButton(SoftkeyName button)
		{
			SendCommand(new { radio = new { command = "buttonRelease", options = button.ToString() } });
		}

		/// <summary>
		/// Reset the radio
		/// </summary>
		public void Reset()
		{
			SendCommand(new { radio = new { command = "reset" } });
		}

		/// <summary>
		/// Send a command to the server
		/// </summary>
		private void SendCommand(object command)
		{
			if (ws?.ReadyState != WebSocketState.Open)
			{
				ErrorOccurred?.Invoke(this, "Cannot send command: WebSocket not connected");
				return;
			}

			var json = JsonConvert.SerializeObject(command);
			ws.Send(json);
		}

		#endregion Radio Control Commands

		#region Event Handlers

		/// <summary>
		/// Handle WebSocket messages
		/// </summary>
		private void OnWebSocketMessage(object sender, MessageEventArgs e)
		{
			try
			{
				var data = JsonConvert.DeserializeObject<Dictionary<string, object>>(e.Data);

				if (data.ContainsKey("status"))
				{
					var statusJson = data["status"].ToString();
					CurrentStatus = JsonConvert.DeserializeObject<RadioStatus>(statusJson);
					StatusReceived?.Invoke(this, CurrentStatus);
				}
				else if (data.ContainsKey("ack"))
				{
					LastAck = data["ack"].ToString();
					AckReceived?.Invoke(this, LastAck);
				}
				else if (data.ContainsKey("nack"))
				{
					LastNack = data["nack"].ToString();
					NackReceived?.Invoke(this, LastNack);
				}
			}
			catch (Exception ex)
			{
				ErrorOccurred?.Invoke(this, $"Failed to parse message: {ex.Message}");
			}
		}

		/// <summary>
		/// Handle audio format negotiation
		/// </summary>
		private void OnAudioFormatsNegotiated(List<AudioFormat> formats)
		{
			negotiatedFormat = formats.Find(f => f.FormatName == CODEC);

			// Verify it's specifically G.722
			if (negotiatedFormat.FormatName != CODEC)
			{
				ErrorOccurred?.Invoke(this, $"Unexpected codec negotiated: {negotiatedFormat.FormatName}");
				pc?.Close("Invalid codec");
				return;
			}

			// Auto-start audio when format is negotiated
			StartAudio();
		}

		/// <summary>
		/// Handle WebRTC connection state changes
		/// </summary>
		private void OnRtcConnectionStateChange(RTCPeerConnectionState state)
		{
			if (state == RTCPeerConnectionState.connected)
			{
				WebRtcConnected?.Invoke(this, EventArgs.Empty);
			}
			else if (state == RTCPeerConnectionState.closed || state == RTCPeerConnectionState.failed)
			{
				WebRtcDisconnected?.Invoke(this, EventArgs.Empty);
			}
		}

		/// <summary>
		/// Handle incoming RTP audio packets
		/// </summary>
		private void OnRtpPacketReceived(System.Net.IPEndPoint ep, SDPMediaTypesEnum media, RTPPacket packet)
		{
			if (media == SDPMediaTypesEnum.audio && audioBuffer != null)
			{
				try
				{
					// Decode audio
					var pcmSamples = rxEncoder.DecodeAudio(packet.Payload, negotiatedFormat);

					// Convert to bytes and add to buffer
					var bytes = new byte[pcmSamples.Length * 2];
					Buffer.BlockCopy(pcmSamples, 0, bytes, 0, bytes.Length);
					audioBuffer.AddSamples(bytes, 0, bytes.Length);
				}
				catch (Exception ex)
				{
					ErrorOccurred?.Invoke(this, $"Failed to process audio: {ex.Message}");
				}
			}
		}

		/// <summary>
		/// Handle microphone audio data for transmission
		/// </summary>
		private void OnMicrophoneDataAvailable(object sender, WaveInEventArgs e)
		{
			if (pc?.connectionState != RTCPeerConnectionState.connected || !IsTransmitting)
				return;

			try
			{
				// Convert bytes to PCM16 samples
				var samples = new short[e.BytesRecorded / 2];
				Buffer.BlockCopy(e.Buffer, 0, samples, 0, e.BytesRecorded);

				// Encode and send
				var encoded = txEncoder.EncodeAudio(samples, negotiatedFormat);
				pc.SendAudio((uint)encoded.Length, encoded);
			}
			catch (Exception ex)
			{
				ErrorOccurred?.Invoke(this, $"Failed to send audio: {ex.Message}");
			}
		}

		#endregion Event Handlers

		#region IDisposable Implementation

		public void Dispose()
		{
			Disconnect();
		}

		#endregion IDisposable Implementation
	}

	#region Supporting Classes

	/// <summary>
	/// Audio device information
	/// </summary>
	public class AudioDeviceInfo
	{
		public int DeviceNumber { get; set; }

		public string Name { get; set; }

		public int Channels { get; set; }
	}

	/// <summary>
	/// Radio status (mirrors server-side RadioStatus class)
	/// </summary>
	public class RadioStatus
	{
		public string Name { get; set; } = "";

		public string Description { get; set; } = "";

		public string ZoneName { get; set; } = "";

		public string ChannelName { get; set; } = "";

		public string CallerId { get; set; } = "";

		public RadioState State { get; set; } = RadioState.Disconnected;

		public ScanState ScanState { get; set; } = ScanState.NotScanning;

		public PriorityState PriorityState { get; set; } = PriorityState.NoPriority;

		public PowerState PowerState { get; set; } = PowerState.LowPower;

		public List<Softkey> Softkeys { get; set; } = new List<Softkey>();

		public bool Monitor { get; set; } = false;

		public bool Direct { get; set; } = false;

		public bool Error { get; set; } = false;

		public bool Secure { get; set; } = false;

		public string ErrorMsg { get; set; } = "";
	}

	/// <summary>
	/// Softkey representation
	/// </summary>
	public class Softkey
	{
		public SoftkeyName Name { get; set; }

		public string Description { get; set; }

		public SoftkeyState State { get; set; }
	}

	/// <summary>
	/// Radio state enumeration
	/// </summary>
	public enum RadioState
	{
		Disconnected,

		Connecting,

		Idle,

		Transmitting,

		Receiving,

		Encrypted,

		Error,

		Disconnecting
	}

	/// <summary>
	/// Scan state enumeration
	/// </summary>
	public enum ScanState
	{
		NotScanning,

		Scanning
	}

	/// <summary>
	/// Priority state enumeration
	/// </summary>
	public enum PriorityState
	{
		NoPriority,

		Priority1,

		Priority2
	}

	/// <summary>
	/// Power state enumeration
	/// </summary>
	public enum PowerState
	{
		LowPower,

		MidPower,

		HighPower
	}

	/// <summary>
	/// Softkey state enumeration
	/// </summary>
	public enum SoftkeyState
	{
		Off,

		On,

		Flashing
	}

	/// <summary>
	/// Softkey name enumeration
	/// </summary>
	public enum SoftkeyName
	{
		CALL, CHAN, CHUP, CHDN, DEL, DIR, EMER, DYNP, HOME, LOCK,

		LPWR, MON, PAGE, PHON, RAB1, RAB2, RCL, SCAN, SEC, SEL,

		SITE, TCH1, TCH2, TCH3, TCH4, TGRP, TMS, TMSQ, ZNUP, ZNDN, ZONE
	}

	#endregion Supporting Classes
}