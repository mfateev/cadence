// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Code generated by thriftrw v1.10.0. DO NOT EDIT.
// @generated

package replicator

import (
	"github.com/uber/cadence/.gen/go/shared"
	"go.uber.org/thriftrw/thriftreflect"
)

// ThriftModule represents the IDL file used to generate this package.
var ThriftModule = &thriftreflect.ThriftModule{
	Name:     "replicator",
	Package:  "github.com/uber/cadence/.gen/go/replicator",
	FilePath: "replicator.thrift",
	SHA1:     "65d6ae6851de5df26234324f7ab273c396949471",
	Includes: []*thriftreflect.ThriftModule{
		shared.ThriftModule,
	},
	Raw: rawIDL,
}

const rawIDL = "// Copyright (c) 2017 Uber Technologies, Inc.\n//\n// Permission is hereby granted, free of charge, to any person obtaining a copy\n// of this software and associated documentation files (the \"Software\"), to deal\n// in the Software without restriction, including without limitation the rights\n// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell\n// copies of the Software, and to permit persons to whom the Software is\n// furnished to do so, subject to the following conditions:\n//\n// The above copyright notice and this permission notice shall be included in\n// all copies or substantial portions of the Software.\n//\n// THE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR\n// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,\n// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE\n// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER\n// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,\n// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN\n// THE SOFTWARE.\n\nnamespace java com.uber.cadence.replicator\n\ninclude \"shared.thrift\"\n\nenum ReplicationTaskType {\n  Domain\n  History\n}\n\nenum DomainOperation {\n  Create\n  Update\n}\n\nstruct DomainTaskAttributes {\n  05: optional DomainOperation domainOperation\n  10: optional string id\n  20: optional shared.DomainInfo info\n  30: optional shared.DomainConfiguration config\n  40: optional shared.DomainReplicationConfiguration replicationConfig\n  50: optional i64 (js.type = \"Long\") configVersion\n  60: optional i64 (js.type = \"Long\") failoverVersion\n}\n\nstruct HistoryTaskAttributes {\n  10: optional string domainId\n  20: optional string workflowId\n  30: optional string runId\n  40: optional i64 (js.type = \"Long\") firstEventId\n  50: optional i64 (js.type = \"Long\") nextEventId\n  60: optional i64 (js.type = \"Long\") version\n  70: optional shared.History history\n  80: optional shared.History newRunHistory\n}\n\nstruct ReplicationTask {\n  10: optional ReplicationTaskType taskType\n  20: optional DomainTaskAttributes domainTaskAttributes\n  30: optional HistoryTaskAttributes historyTaskAttributes\n}\n\n"
