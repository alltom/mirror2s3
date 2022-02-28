package mirror2s3

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/s3blob"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"os"
	"os/exec"
	"path"
)

var (
	IgnoredFiles = map[string]struct{}{
		".gitignore": struct{}{},
	}
)

type Mirror struct {
	gitPath string
	siteSourcePath string
	awsProfile     string
	awsRegion      string
	bucketURL      string
}

func New(options ...func(*Mirror)) *Mirror {
	m := &Mirror{
		gitPath : "/usr/bin/git",
	}
	for _, opt := range options {
		opt(m)
	}
	return m
}

func WithGitRepoRoot(path string) func(*Mirror) {
	return func(m *Mirror) {
		m.siteSourcePath = path
	}
}

// Example: example.com
func WithAwsProfile(name string) func(*Mirror) {
	return func(m *Mirror) {
		m.awsProfile = name
	}
}

// Example: us-east-1
func WithAwsRegion(name string) func(*Mirror) {
	return func(m *Mirror) {
		m.awsRegion = name
	}
}

// Example: s3://example.com
func WithBucketURL(url string) func(*Mirror) {
	return func(m *Mirror) {
		m.bucketURL = url
	}
}

func (m *Mirror) Run(ctx context.Context) error {
	os.Setenv("AWS_REGION", m.awsRegion)
	os.Setenv("AWS_PROFILE", m.awsProfile)

	bucket, err := blob.OpenBucket(ctx, m.bucketURL)
	if err != nil {
		return fmt.Errorf("open bucket: %v", err)
	}
	defer bucket.Close()

	hashes := map[string][]byte{}
	itr := bucket.List(nil)
	for {
		obj, err := itr.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if obj.MD5 != nil {
			hashes[obj.Key] = obj.MD5
		}
	}

	r, err := m.getSiteTar()
	if err != nil {
		return fmt.Errorf("get site tar: %v", err)
	}

	for {
		header, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("get next file in tar: %v", err)
		}

		if header.Typeflag != tar.TypeReg {
			continue
		}
		if _, ok := IgnoredFiles[header.Name]; ok {
			continue
		}

		data, err := ioutil.ReadAll(r)
		if err != nil {
			return fmt.Errorf(`read file "%s": %v`, header.Name, err)
		}

		if remoteSum, ok := hashes[header.Name]; ok {
			localSum := md5.Sum(data)
			if bytes.Equal(localSum[:], remoteSum) {
				log.Printf("skipping %s…", header.Name)
				continue
			}
		}

		log.Printf("uploading %s…", header.Name)

		var options *blob.WriterOptions
		if contentType := mime.TypeByExtension(path.Ext(header.Name)); contentType != "" {
			options = &blob.WriterOptions{ContentType: contentType}
		}
		if err = bucket.WriteAll(ctx, header.Name, data, options); err != nil {
			return fmt.Errorf("upload file: %v", err)
		}
	}

	return nil
}

func (m *Mirror) getSiteTar() (*tar.Reader, error) {
	cmd := &exec.Cmd{
		Path:   m.gitPath,
		Args:   []string{m.gitPath, "archive", "--format=tar", "HEAD"},
		Env:    []string{},
		Dir:    m.siteSourcePath,
		Stderr: os.Stderr,
	}
	tarf, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("get git stdout: %v", err)
	}
	if err = cmd.Start(); err != nil {
		return nil, fmt.Errorf("start git: %v", err)
	}

	return tar.NewReader(tarf), nil
}
